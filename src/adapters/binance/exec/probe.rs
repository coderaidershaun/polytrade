//! Startup probe: signed account read proves key can trade before threads spawn. Without it,
//! first bad-key evidence is rejected order mid-session with live strategy.

use std::path::Path;

use crate::adapters::binance::rest::{
    AccountInfo, BinanceEnv, RestError, SignedRestClient, SignedRestConfig,
};
use crate::adapters::rest_quiet::SharedRestQuiet;
use crate::info;
use crate::secrets::{Credentials, EnvFile, SecretError};

use super::BINANCE_CREDENTIAL_VARIABLES;

const REQUIRED_ACCOUNT_TYPE: &str = "SPOT";

#[derive(thiserror::Error, Debug)]
pub enum ProbeError {
    #[error("binance credentials could not be loaded — execution needs a key it can sign with")]
    Credentials(#[from] SecretError),
    #[error("binance account probe failed — refusing to start without proof the key can trade")]
    Unreachable {
        #[source]
        source: RestError,
    },
    #[error(
        "the binance api key cannot trade (canTrade=false) — enable spot trading on the key, or run with execution disabled"
    )]
    TradingDisabled,
    #[error(
        "the binance api key is a {account_type} account, not {REQUIRED_ACCOUNT_TYPE} — this engine trades spot only"
    )]
    WrongAccountType { account_type: Box<str> },
}

/// Startup gate result (probe + client + credentials). Caller trades with same resources.
pub struct ExecutionPreflight {
    /// The probed client, offset already learned, carried on so the run trades with the connection
    /// the gate proved rather than a fresh one.
    pub rest: SignedRestClient,
    /// Second key copy (WS=param signer, REST=header). One read, no Clone secret.
    pub credentials: Credentials,
    pub probe: AccountInfo,
}

/// Load creds, clock offset, prove key can trade. BEFORE spawning (not for recorders).
///
/// `quiet` is the venue's cool-off window, shared with the market-data actor because both spend the
/// one per-IP budget; a caller with no market-data half hands in a window of its own.
///
/// # Errors
/// Creds absent, unreachable/rejected, or permission refusal.
pub async fn preflight_execution(
    env: BinanceEnv,
    quiet: SharedRestQuiet,
) -> Result<ExecutionPreflight, ProbeError> {
    let secrets = EnvFile::load(Path::new(EnvFile::DEFAULT_PATH))?;
    let credentials = secrets.resolve_credentials(&BINANCE_CREDENTIAL_VARIABLES)?;
    let mut client = SignedRestClient::new(
        secrets.resolve_credentials(&BINANCE_CREDENTIAL_VARIABLES)?,
        SignedRestConfig {
            env,
            ..SignedRestConfig::default()
        },
        quiet,
    )
    .map_err(|source| ProbeError::Unreachable { source })?;
    let offset = client
        .sync_clock()
        .await
        .map_err(|source| ProbeError::Unreachable { source })?;
    info!(
        "binance venue clock offset {}us",
        offset.correction().micros()
    );
    let probe = probe_account(&mut client).await?;
    Ok(ExecutionPreflight {
        rest: client,
        credentials,
        probe,
    })
}

/// Prove key can trade spot (asserts canTrade + type). Logs permissions (account-specific).
///
/// # Errors
/// Venue/key rejected, trading disabled, or wrong account type (not SPOT).
async fn probe_account(client: &mut SignedRestClient) -> Result<AccountInfo, ProbeError> {
    let account = client
        .account()
        .await
        .map_err(|source| ProbeError::Unreachable { source })?;

    if !account.can_trade {
        return Err(ProbeError::TradingDisabled);
    }
    if &*account.account_type != REQUIRED_ACCOUNT_TYPE {
        return Err(ProbeError::WrongAccountType {
            account_type: account.account_type.clone(),
        });
    }

    report(&account);
    Ok(account)
}

/// Fees logged at INFO every run (maker rate is economically decisive, read not inferred).
fn report(account: &AccountInfo) {
    let rates = &account.commission_rates;
    info!(
        "binance account ok: spot, can trade, permissions {:?}",
        account.permissions
    );
    info!(
        "binance commission: maker {} ({}), taker {} ({})",
        rates.maker,
        basis_points(&rates.maker),
        rates.taker,
        basis_points(&rates.taker)
    );
    for balance in account.funded_balances() {
        info!(
            "binance balance {}: {} free, {} locked",
            balance.asset, balance.free, balance.locked
        );
    }
}

/// Display only — venue decimal string is truth.
fn basis_points(rate: &str) -> String {
    match rate.parse::<f64>() {
        Ok(parsed) => format!("{:.2} bps", parsed * 10_000.0),
        Err(_) => "unparsed".to_owned(),
    }
}
