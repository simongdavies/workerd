// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use super::*;

#[test]
fn configured_selection_checks_kind_index_and_exact_name() {
    let admission = WorkerAdmission::configured(7, "app".to_owned()).unwrap();
    for (configured, index, name) in [(false, 7, "app"), (true, 8, "app"), (true, 7, "other")] {
        assert_eq!(
            admission.begin_worker(configured, index, name),
            Err(AdmissionError::WrongSource)
        );
    }
    admission.begin_worker(true, 7, "app").unwrap();
    admission.admit_isolate(true, 7, "app").unwrap();
}

#[test]
fn dynamic_selection_cannot_admit_a_configured_worker_or_different_child() {
    let admission = WorkerAdmission::dynamic("loader:child".to_owned()).unwrap();
    assert_eq!(
        admission.begin_worker(true, 0, "loader:child"),
        Err(AdmissionError::WrongSource)
    );
    assert_eq!(
        admission.begin_worker(false, 0, "loader:other"),
        Err(AdmissionError::WrongSource)
    );
    admission.begin_worker(false, 0, "loader:child").unwrap();
    admission.admit_isolate(false, 0, "loader:child").unwrap();
}

#[test]
fn reservation_prevents_concurrent_startup_before_either_isolate_exists() {
    let admission = WorkerAdmission::configured(0, "app".to_owned()).unwrap();
    admission.begin_worker(true, 0, "app").unwrap();
    assert_eq!(
        admission.begin_worker(true, 0, "app"),
        Err(AdmissionError::AlreadyStarted)
    );
    admission.admit_isolate(true, 0, "app").unwrap();
}

#[test]
fn initialization_failure_does_not_restore_admission() {
    let admission = WorkerAdmission::dynamic("child".to_owned()).unwrap();
    admission.begin_worker(false, 0, "child").unwrap();
    // The source fetch or compilation fails: there is deliberately no reset operation.
    assert_eq!(
        admission.begin_worker(false, 0, "child"),
        Err(AdmissionError::AlreadyStarted)
    );
}

#[test]
fn actual_creation_requires_reservation_and_is_one_shot() {
    let admission = WorkerAdmission::configured(0, "app".to_owned()).unwrap();
    assert_eq!(
        admission.admit_isolate(true, 0, "app"),
        Err(AdmissionError::NotReserved)
    );
    admission.begin_worker(true, 0, "app").unwrap();
    admission.admit_isolate(true, 0, "app").unwrap();
    assert_eq!(
        admission.admit_isolate(true, 0, "app"),
        Err(AdmissionError::NotReserved)
    );
    assert_eq!(
        admission.begin_worker(true, 0, "app"),
        Err(AdmissionError::AlreadyStarted)
    );
}

#[test]
fn source_cannot_change_between_reservation_and_actual_creation() {
    let admission = WorkerAdmission::configured(0, "app".to_owned()).unwrap();
    admission.begin_worker(true, 0, "app").unwrap();
    assert_eq!(
        admission.admit_isolate(false, 0, "app"),
        Err(AdmissionError::WrongSource)
    );
    assert_eq!(
        admission.admit_isolate(true, 0, "other"),
        Err(AdmissionError::WrongSource)
    );
    admission.admit_isolate(true, 0, "app").unwrap();
}

#[test]
fn separate_factories_do_not_share_admission_state() {
    let first = WorkerAdmission::configured(0, "app".to_owned()).unwrap();
    let second = WorkerAdmission::configured(0, "app".to_owned()).unwrap();
    first.begin_worker(true, 0, "app").unwrap();
    first.admit_isolate(true, 0, "app").unwrap();
    second.begin_worker(true, 0, "app").unwrap();
    second.admit_isolate(true, 0, "app").unwrap();
}

#[test]
fn an_empty_selected_name_is_rejected() {
    assert!(matches!(
        WorkerAdmission::configured(0, String::new()),
        Err(AdmissionError::EmptyName)
    ));
    assert!(matches!(
        WorkerAdmission::dynamic(String::new()),
        Err(AdmissionError::EmptyName)
    ));
}
