//! One command, one read-only report: is this host allowed to trade, does the wallet key mint L2
//! credentials, which wallet type is this account, what does it hold, and what is resting.
//!
//! It never places, amends or cancels an order. It is the guardrail the rest of the Polymarket
//! execution work is built behind, so it runs before that work exists and again between phases.

use std::path::Path;

use anyhow::{Context, Result};
use polysim::adapters::polymarket::exec::POLYMARKET_CREDENTIAL_VARIABLES;
use polysim::adapters::polymarket::exec::probe::{
    CredentialSource, Probe, ProbeReport, ResponseShape, WalletCandidate,
};
use polysim::adapters::polymarket::exec::sign::key::SigningKey;
use polysim::secrets::EnvFile;

#[tokio::main]
async fn main() -> Result<()> {
    let env_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| EnvFile::DEFAULT_PATH.to_owned());
    let env = EnvFile::load(Path::new(&env_path))
        .with_context(|| format!("loading credentials from {env_path}"))?;

    let secret = env
        .resolve(POLYMARKET_CREDENTIAL_VARIABLES.api_secret_env)
        .with_context(|| {
            format!(
                "reading the wallet private key from {}",
                POLYMARKET_CREDENTIAL_VARIABLES.api_secret_env
            )
        })?;
    let key = SigningKey::from_secret(&secret).context("parsing the wallet private key")?;

    let configured_signer = env
        .resolve(POLYMARKET_CREDENTIAL_VARIABLES.api_key_env)
        .ok()
        .and_then(|value| String::from_utf8(value.expose_bytes().to_vec()).ok());

    let report = Probe::new(key)
        .context("building the polymarket probe")?
        .run()
        .await
        .context("running the polymarket probe")?;

    print_report(&report, configured_signer.as_deref());
    Ok(())
}

fn print_report(report: &ProbeReport, configured_signer: Option<&str>) {
    println!("polymarket account probe (read-only)");
    println!();

    println!("  region");
    println!(
        "    geoblock         blocked={} country={} region={} ip={}",
        report.geoblock.blocked,
        blank_as_dash(&report.geoblock.country),
        blank_as_dash(&report.geoblock.region),
        blank_as_dash(&report.geoblock.ip)
    );
    println!(
        "    order placement  {}",
        if report.geoblock.blocked {
            "REFUSED from this host — market data still works, execution does not"
        } else {
            "permitted from this host"
        }
    );
    println!();

    println!("  venue");
    println!("    protocol         {}", report.protocol_version);
    println!(
        "    server time      {} (local clock {:+}s)",
        report.server_time_secs, report.clock_skew_secs
    );
    match &report.is_closed_only {
        Ok(true) => println!("    closed-only      YES — this account may only reduce positions"),
        Ok(false) => println!("    closed-only      no — this account may open positions"),
        Err(failure) => println!("    closed-only      unreadable: {failure}"),
    }
    println!();

    println!("  identity");
    println!("    signer           {}", report.signer.to_checksum_hex());
    match configured_signer {
        Some(configured) if configured.eq_ignore_ascii_case(&report.signer.to_checksum_hex()) => {
            println!("    configured       matches SIGNER_ADDRESS");
        }
        Some(configured) => println!(
            "    configured       MISMATCH — SIGNER_ADDRESS says {configured}, the key derives \
             the address above"
        ),
        None => println!("    configured       SIGNER_ADDRESS not set"),
    }
    println!(
        "    l2 credentials   {} at runtime (never stored)",
        match report.api_key_source {
            CredentialSource::Derived => "derived",
            CredentialSource::Created => "created",
        }
    );
    println!();

    println!("  wallet type");
    for candidate in &report.wallet_candidates {
        print_candidate(candidate);
    }
    match report.funded_wallet() {
        Some(found) => println!(
            "    -> signatureType {} holds the collateral; that is this account's wallet type",
            found.signature_type.code()
        ),
        None => println!(
            "    -> no signature type reported a non-zero balance: either the account is empty or \
             the maker address differs from the signer and must be supplied"
        ),
    }
    println!();

    println!("  book");
    match &report.open_orders {
        Ok(orders) => println!(
            "    open orders      {} (response shape: {})",
            orders.count,
            match orders.shape {
                ResponseShape::Wrapped => "{data:[…]} wrapped",
                ResponseShape::BareArray => "bare array",
            }
        ),
        Err(failure) => println!("    open orders      unreadable: {failure}"),
    }
    println!(
        "    rate limit       remaining={} warning={}",
        report
            .rate_limit
            .remaining
            .map_or_else(|| "-".to_owned(), |budget| budget.to_string()),
        report.rate_limit.warning.as_deref().unwrap_or("-")
    );
}

fn print_candidate(candidate: &WalletCandidate) {
    let label = format!("    signatureType {}", candidate.signature_type.code());
    match &candidate.collateral {
        Ok(collateral) => {
            println!(
                "{label}    balance={} pUSD (1e6) allowances={}",
                collateral.balance,
                if collateral.allowances.is_empty() {
                    "none".to_owned()
                } else {
                    collateral
                        .allowances
                        .iter()
                        .map(|(exchange, amount)| format!("{exchange}={}", approve_state(amount)))
                        .collect::<Vec<_>>()
                        .join(" ")
                }
            );
        }
        Err(failure) => println!("{label}    refused: {failure}"),
    }
}

/// Allowances are set to `maxUint256`, so the digit count says more than the digits do.
fn approve_state(amount: &str) -> String {
    match amount.trim() {
        "0" | "" => "unset".to_owned(),
        set if set.len() >= 70 => "max".to_owned(),
        set => set.to_owned(),
    }
}

fn blank_as_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}
