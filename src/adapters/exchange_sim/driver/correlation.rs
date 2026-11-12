//! Correlation between simulated requests and answers. The venue mints its answers from the order
//! it holds rather than from the request that asked, so an answer carries no request id and the
//! match has to be made on the shape of the pair.

use super::super::core::schedule::{AnswerSubject, VenueAnswer};
use crate::adapters::exec::ExecRequest;

pub(super) fn request_matches_answer(request: ExecRequest, answer: &VenueAnswer) -> bool {
    match (request, answer) {
        (ExecRequest::Place { client_id, .. }, VenueAnswer::PlaceAccepted(order)) => {
            client_id == order.client_id
        }
        (
            ExecRequest::Place { client_id, .. },
            VenueAnswer::Refused {
                client_id: answered,
                about: AnswerSubject::Place,
                ..
            },
        ) => client_id == *answered,
        (ExecRequest::Cancel { client_id, .. }, VenueAnswer::CancelAccepted(order)) => {
            client_id == order.client_id
        }
        (
            ExecRequest::Cancel { client_id, .. },
            VenueAnswer::Refused {
                client_id: answered,
                about: AnswerSubject::Cancel,
                ..
            },
        ) => client_id == *answered,
        (ExecRequest::AmendQty { client_id, .. }, VenueAnswer::AmendAccepted(order)) => {
            client_id == order.client_id
        }
        (
            ExecRequest::AmendQty { client_id, .. },
            VenueAnswer::Refused {
                client_id: answered,
                about: AnswerSubject::Amend,
                ..
            },
        ) => client_id == *answered,
        (ExecRequest::OrderStatus { client_id, .. }, VenueAnswer::Status { order, .. }) => {
            client_id == order.client_id
        }
        (
            ExecRequest::OrderStatus { client_id, .. },
            VenueAnswer::Refused {
                client_id: answered,
                about: AnswerSubject::Query,
                ..
            },
        ) => client_id == *answered,
        (ExecRequest::OpenOrders { .. }, VenueAnswer::OpenOrders(_)) => true,
        _ => false,
    }
}
