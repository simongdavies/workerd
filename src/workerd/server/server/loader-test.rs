// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use super::*;

struct Hosted {
    unlinks: Rc<Cell<usize>>,
    entrypoints: Rc<RefCell<Vec<Option<String>>>>,
}

impl HostedWorker for Hosted {
    fn entrypoint(
        &self,
        name: Option<&str>,
        _props: KjOwn<Frankenvalue>,
    ) -> Result<Rc<dyn Channel>> {
        self.entrypoints.borrow_mut().push(name.map(str::to_owned));
        Err(kj::failed!("fixture endpoint refused"))
    }

    fn actor_class(
        &self,
        _name: Option<&str>,
        _props: KjOwn<Frankenvalue>,
    ) -> Result<Rc<dyn ActorClass>> {
        Err(kj::failed!("fixture class refused"))
    }

    fn unlink(&self) {
        self.unlinks.set(self.unlinks.get() + 1);
    }
}

fn hosted(unlinks: Rc<Cell<usize>>) -> LoadedWorker {
    LoadedWorker::Hosted(Rc::new(Hosted {
        unlinks,
        entrypoints: Rc::new(RefCell::new(Vec::new())),
    }))
}

#[test]
fn closed_dynamic_startup_unlinks_actual_result_instead_of_publishing_it() {
    let unlinks = Rc::new(Cell::new(0));
    let result = LoadedWorker::complete_startup(Ok(hosted(Rc::clone(&unlinks))), true);
    assert!(result.is_err());
    assert_eq!(unlinks.get(), 1);
}

#[test]
fn open_dynamic_startup_keeps_the_same_hosted_result_without_local_fallback() {
    let unlinks = Rc::new(Cell::new(0));
    let result = LoadedWorker::complete_startup(Ok(hosted(Rc::clone(&unlinks))), false).unwrap();
    assert!(matches!(result, LoadedWorker::Hosted(_)));
    assert_eq!(unlinks.get(), 0);
    result.unlink();
    assert_eq!(unlinks.get(), 1);
}

#[test]
fn failed_dynamic_startup_preserves_original_failure() {
    let error = LoadedWorker::complete_startup(Err(kj::failed!("owned startup failure")), false)
        .err()
        .unwrap();
    assert_eq!(error.description(), "owned startup failure");
}

#[test]
fn hosted_entrypoint_refusal_keeps_original_name_and_does_not_instantiate_local_worker() {
    let entrypoints = Rc::new(RefCell::new(Vec::new()));
    let target = LoadedWorker::Hosted(Rc::new(Hosted {
        unlinks: Rc::new(Cell::new(0)),
        entrypoints: Rc::clone(&entrypoints),
    }));
    let error = target
        .entrypoint(Some("Admin"), ffi::frankenvalue_new())
        .err()
        .unwrap();
    assert_eq!(error.description(), "fixture endpoint refused");
    assert_eq!(&*entrypoints.borrow(), &[Some("Admin".to_owned())]);
}
