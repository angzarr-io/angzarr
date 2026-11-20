//! What the expansion refuses, and why, by its messages: the
//! `compile_fail` examples pin that each misuse fails to compile; these pin
//! that it fails for its own reason.

use proc_macro2::TokenStream as Tokens;
use quote::quote;

use super::{domain, flow, Flow, Kind};

/// The aggregate expansion's error for `item`, under `attr`.
fn refused(kind: Kind, attr: Tokens, item: Tokens) -> String {
    match domain(attr, item, kind) {
        Ok(_) => panic!("expanded, but should be refused"),
        Err(error) => error.to_string(),
    }
}

fn lamp(item: Tokens) -> String {
    refused(Kind::Aggregate, quote!(domain = "lamp"), item)
}

#[test]
fn a_second_handler_for_one_type_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Unrefusable>> { todo!() }
            #[handles_fact(On)]
            fn on_again(&self, fact: On, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
        }
    });
    assert!(error.contains("a second handler for this type"), "{error}");
}

/// A command that lists no refusal cannot refuse (decision 14.95): a
/// handler of it failing with a rejection of its own is refused.
#[test]
fn a_command_listing_no_refusal_that_refuses_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Unrefusable>> { todo!() }
            #[handles_cmd(Off)]
            fn off(&self, cmd: Off, ctx: &Context) -> Result<Dark, WireError> { todo!() }
        }
    });
    assert!(
        error.contains("a command that lists no refusal cannot refuse"),
        "{error}"
    );
}

/// A command that lists no refusal fails with `Failed<Unrefusable>`, and
/// the domain's rejection is a typed refusal.
#[test]
fn a_command_listing_no_refusal_fails_unrefusable() {
    let expanded = domain(
        quote!(domain = "lamp"),
        quote! {
            impl Lamp {
                #[handles_cmd(On)]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Unrefusable>> { todo!() }
            }
        },
        Kind::Aggregate,
    )
    .expect("expands")
    .to_string();
    assert!(
        expanded.contains("type Rejection = :: angzarr_embedded :: markup :: TypedRefusal"),
        "{expanded}"
    );
}

/// Only a `Failed` of one argument is looked through: another wrapper, or
/// a `Failed` of two, is the refusal it names, and refused.
#[test]
fn only_a_failed_of_one_argument_is_looked_through() {
    for error_type in [
        quote!(Unfailed<Unrefusable>),
        quote!(Failed<Unrefusable, Other>),
    ] {
        let error = lamp(quote! {
            impl Lamp {
                #[handles_cmd(On)]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, #error_type> { todo!() }
            }
        });
        assert!(
            error.contains("a command that lists no refusal cannot refuse"),
            "{error_type}: {error}"
        );
    }
}

#[test]
fn a_refusing_handler_may_fail_with_its_choice() {
    domain(
        quote!(domain = "lamp"),
        quote! {
            impl Lamp {
                #[handles_cmd(On, refuses(Burnt))]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Refuses<Self, On>>> { todo!() }
            }
        },
        Kind::Aggregate,
    )
    .expect("expands");
}

#[test]
fn a_refusing_handler_failing_with_another_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On, refuses(Burnt))]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Burnt>> { todo!() }
        }
    });
    assert!(
        error.contains("`Result<_, Failed<Refuses<Self, Cmd>>>`"),
        "{error}"
    );
}

#[test]
fn a_handler_taking_mut_self_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&mut self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Unrefusable>> { todo!() }
        }
    });
    assert!(error.contains("a handler takes `&self`"), "{error}");
}

#[test]
fn a_handler_taking_self_by_value_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Unrefusable>> { todo!() }
        }
    });
    assert!(error.contains("a handler takes `&self`"), "{error}");
}

#[test]
fn an_applier_taking_self_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[applies(Lit)]
            fn lit(&self, event: Lit) {}
        }
    });
    assert!(error.contains("an applier takes `&mut self`"), "{error}");
}

#[test]
fn a_command_handler_without_a_result_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Option<Lit> { todo!() }
        }
    });
    assert!(
        error.contains("a command handler returns `Result<_, Failed<Unrefusable>>`"),
        "{error}"
    );
}

#[test]
fn a_refusing_handler_whose_err_is_not_its_choice_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On, refuses(Burnt))]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Burnt> { todo!() }
        }
    });
    assert!(error.contains("`Result<_, Refuses<Self, Cmd>>`"), "{error}");
}

#[test]
fn a_fact_handler_returning_a_result_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_fact(Cut)]
            fn cut(&self, fact: Cut, ctx: &Context) -> Result<Dark, Infallible> { todo!() }
        }
    });
    assert!(
        error.contains("a fact handler returns `Result<FactEmit<Self>, Bug>`"),
        "{error}"
    );
}

/// A fact's handler returns `Result<FactEmit<Self>, Bug>`: the router's
/// types, its one failure a bug.
#[test]
fn a_fact_handler_returning_a_fact_emit_is_accepted() {
    let expanded = domain(
        quote!(domain = "lamp"),
        quote! {
            impl Lamp {
                #[handles_fact(Cut)]
                fn cut(&self, fact: Cut, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
            }
        },
        Kind::Aggregate,
    )
    .expect("expands")
    .to_string();
    assert!(expanded.contains(". on_fact ("), "{expanded}");
    assert!(expanded.contains("Ingests < Cut >"), "{expanded}");
}

/// The kind is the attribute's, not the return type's: an `Emit` is a
/// command's, and a fact's handler returning one is refused, saying why.
#[test]
fn a_fact_handler_returning_an_emit_is_refused() {
    for output in [
        quote!(Emit<Self>),
        quote!(Result<Emit<Self>, Bug>),
        quote!(Record<Self>),
    ] {
        let error = lamp(quote! {
            impl Lamp {
                #[handles_fact(Cut)]
                fn cut(&self, fact: Cut, ctx: &Context) -> #output { todo!() }
            }
        });
        assert!(
            error.contains("a fact handler returns `Result<FactEmit<Self>, Bug>`"),
            "{error}"
        );
    }
}

/// A fact is never refused: a `Refuses` (or any error but a `Bug`) in its
/// return is refused.
#[test]
fn a_fact_handler_failing_with_a_refusal_is_refused() {
    for error_type in [
        quote!(Refuses<Self, Cut>),
        quote!(Failed<Unrefusable>),
        quote!(Infallible),
    ] {
        let error = lamp(quote! {
            impl Lamp {
                #[handles_fact(Cut)]
                fn cut(&self, fact: Cut, ctx: &Context) -> Result<FactEmit<Self>, #error_type> {
                    todo!()
                }
            }
        });
        assert!(error.contains("a fact cannot be refused"), "{error}");
        assert!(error.contains("`Bug`"), "{error}");
    }
}

/// A command handler returning a fact's types is refused as a command
/// handler is: it lists no refusal, so it fails `Failed<Unrefusable>`.
#[test]
fn a_command_handler_returning_a_fact_emit_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
        }
    });
    assert!(
        error.contains("a command that lists no refusal cannot refuse"),
        "{error}"
    );
}

/// The expansion of `marker` (`handles_cmd` or `handles_fact`) on a method
/// with `signature`, in an aggregate or a process manager.
fn handled(kind: Kind, marker: &str, args: Tokens, signature: Tokens) -> Result<String, String> {
    let attr = match kind {
        Kind::Aggregate => quote!(domain = "lamp"),
        Kind::ProcessManager => quote!(domain = "timer"),
    };
    let marker = quote::format_ident!("{marker}");
    domain(
        attr,
        quote! {
            impl Lamp {
                #[#marker(#args)]
                #signature
            }
        },
        kind,
    )
    .map(|expanded| expanded.to_string())
    .map_err(|error| error.to_string())
}

/// A command handler's signature and a fact handler's, for `handled`.
fn command_signature() -> Tokens {
    quote!(
        fn cut(&self, cmd: Cut, ctx: &Context) -> Result<Emit<Self>, Failed<Unrefusable>> {
            todo!()
        }
    )
}

fn fact_signature() -> Tokens {
    quote!(
        fn cut(&self, fact: Cut, ctx: &Context) -> Result<FactEmit<Self>, Bug> {
            todo!()
        }
    )
}

/// The attribute names the kind: the same message under `handles_cmd` is
/// a command's route and under `handles_fact` a fact's.
#[test]
fn handles_cmd_routes_a_command_and_handles_fact_a_fact() {
    let command = handled(
        Kind::Aggregate,
        "handles_cmd",
        quote!(Cut),
        command_signature(),
    )
    .expect("expands");
    assert!(command.contains(". on_command ("), "{command}");
    assert!(!command.contains(". on_fact ("), "{command}");
    assert!(command.contains("Decides < Cut >"), "{command}");
    let fact = handled(
        Kind::Aggregate,
        "handles_fact",
        quote!(Cut),
        fact_signature(),
    )
    .expect("expands");
    assert!(fact.contains(". on_fact ("), "{fact}");
    assert!(!fact.contains(". on_command ("), "{fact}");
    assert!(fact.contains("Ingests < Cut >"), "{fact}");
}

/// The fact's route decodes with the router's bug conversion and answers
/// the router's `FactEmit` (`markup::run_fact`).
#[test]
fn a_fact_route_runs_through_run_fact() {
    let fact = handled(
        Kind::Aggregate,
        "handles_fact",
        quote!(Cut),
        fact_signature(),
    )
    .expect("expands");
    assert!(fact.contains("run_fact :: < Lamp , Cut >"), "{fact}");
}

/// An argument that does not apply to the kind is refused, naming it.
#[test]
fn refuses_on_a_fact_is_refused_naming_the_kind() {
    let error = handled(
        Kind::Aggregate,
        "handles_fact",
        quote!(Cut, refuses(Burnt)),
        fact_signature(),
    )
    .expect_err("refused");
    assert!(error.contains("a fact handler"), "{error}");
    assert!(error.contains("`refuses`"), "{error}");
}

#[test]
fn rooted_by_on_a_fact_is_refused_naming_the_kind() {
    let error = handled(
        Kind::Aggregate,
        "handles_fact",
        quote!(Cut, rooted_by = key),
        fact_signature(),
    )
    .expect_err("refused");
    assert!(error.contains("a fact handler"), "{error}");
    assert!(error.contains("`rooted_by`"), "{error}");
}

#[test]
fn starts_on_an_aggregate_is_refused_naming_the_kind() {
    for (marker, signature, kind) in [
        ("handles_cmd", command_signature(), "command handler"),
        ("handles_fact", fact_signature(), "fact handler"),
    ] {
        let error =
            handled(Kind::Aggregate, marker, quote!(Cut, starts), signature).expect_err("refused");
        assert!(error.contains("`starts` is a process manager's"), "{error}");
        assert!(error.contains(kind), "{error}");
    }
}

#[test]
fn starts_on_a_process_manager_is_accepted_for_both_kinds() {
    for (marker, signature) in [
        ("handles_cmd", command_signature()),
        ("handles_fact", fact_signature()),
    ] {
        let expanded =
            handled(Kind::ProcessManager, marker, quote!(Cut, starts), signature).expect("expands");
        assert!(
            expanded.contains("const STARTS : & 'static [:: angzarr_embedded :: markup :: TypeName] = & [:: angzarr_embedded :: markup :: TypeName :: of :: << Cut as"),
            "{expanded}"
        );
    }
}

/// A command handler still holds to its refusals.
#[test]
fn a_command_is_held_to_its_refusals() {
    let error = handled(
        Kind::Aggregate,
        "handles_cmd",
        quote!(Cut),
        quote!(
            fn cut(&self, cmd: Cut, ctx: &Context) -> Result<Emit<Self>, Burnt> {
                todo!()
            }
        ),
    )
    .expect_err("refused");
    assert!(
        error.contains("a command that lists no refusal cannot refuse"),
        "{error}"
    );
}

/// A second handler for one type is refused across the two attributes.
#[test]
fn the_two_attributes_share_the_one_handler_per_type_rule() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Emit<Self>, Failed<Unrefusable>> { todo!() }
            #[handles_fact(On)]
            fn on_again(&self, fact: On, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
        }
    });
    assert!(error.contains("a second handler for this type"), "{error}");
}

/// A process manager's `starting_types` (the descriptor's, when the
/// generator gives them) is cross-checked against its `starts` markers; with
/// none given, nothing is checked.
fn timer(args: Tokens) -> Result<String, String> {
    domain(
        quote!(domain = "timer", #args),
        quote! {
            impl Timer {
                #[handles_cmd(Start, starts)]
                fn start(&self, cmd: Start, ctx: &Context) -> Result<Emit<Self>, Failed<Unrefusable>> { todo!() }
                #[handles_fact(Lapse, starts)]
                fn lapse(&self, fact: Lapse, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
                #[handles_fact(Ring)]
                fn ring(&self, fact: Ring, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
            }
        },
        Kind::ProcessManager,
    )
    .map(|expanded| expanded.to_string())
    .map_err(|error| error.to_string())
}

#[test]
fn starting_types_that_agree_with_the_starts_markers_expand() {
    timer(quote!(starting_types = [Lapse, wire::Start])).expect("expands");
}

#[test]
fn without_starting_types_the_starts_markers_are_not_cross_checked() {
    timer(quote!()).expect("expands");
}

#[test]
fn a_starting_type_no_input_starts_is_refused() {
    let error = timer(quote!(starting_types = [Start, Lapse, Ring])).expect_err("refused");
    assert!(error.contains("`Ring`"), "{error}");
    assert!(error.contains("`starts`"), "{error}");
}

#[test]
fn a_starts_marker_the_starting_types_omit_is_refused() {
    let error = timer(quote!(starting_types = [Start])).expect_err("refused");
    assert!(error.contains("`Lapse`"), "{error}");
    assert!(error.contains("starting_types"), "{error}");
}

#[test]
fn starting_types_on_an_aggregate_is_refused() {
    let error = refused(
        Kind::Aggregate,
        quote!(domain = "lamp", starting_types = [On]),
        quote! {
            impl Lamp {
                #[handles_cmd(On)]
                fn on(&self, cmd: On, ctx: &Context) -> Result<Emit<Self>, Failed<Unrefusable>> { todo!() }
            }
        },
    );
    assert!(
        error.contains("`starting_types` is a process manager's"),
        "{error}"
    );
}

/// A domain takes no `#[on_refused]`: a refusal reaches the policy's
/// Translate callback, never a domain.
#[test]
fn an_on_refused_on_a_domain_is_refused() {
    for marker in [quote!(#[on_refused]), quote!(#[on_refused(On)])] {
        let error = lamp(quote! {
            impl Lamp {
                #marker
                fn on_refused(&self, refused: &Refused) -> Result<Dark, Infallible> { todo!() }
            }
        });
        assert!(error.contains("a domain has no `#[on_refused]`"), "{error}");
    }
}

#[test]
fn starts_on_an_aggregate_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_cmd(On, starts)]
            fn on(&self, cmd: On, ctx: &Context) -> Result<Lit, Failed<Unrefusable>> { todo!() }
        }
    });
    assert!(error.contains("`starts` is a process manager's"), "{error}");
}

#[test]
fn starts_on_an_applier_is_refused() {
    let error = refused(
        Kind::ProcessManager,
        quote!(domain = "timer"),
        quote! {
            impl Timer {
                #[applies(Rang, starts)]
                fn rang(&mut self, event: Rang) {}
            }
        },
    );
    assert!(
        error.contains("`starts` marks a process manager's command or fact handler only"),
        "{error}"
    );
}

#[test]
fn refuses_on_a_fact_handler_is_refused() {
    let error = lamp(quote! {
        impl Lamp {
            #[handles_fact(Cut, refuses(Burnt))]
            fn cut(&self, fact: Cut, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
        }
    });
    assert!(error.contains("a fact is never refused"), "{error}");
}

#[test]
fn a_process_manager_marking_no_input_starts_declares_no_starting_type() {
    let expanded = domain(
        quote!(domain = "timer"),
        quote! {
            impl Timer {
                #[handles_fact(Ring)]
                fn ring(&self, fact: Ring, ctx: &Context) -> Result<FactEmit<Self>, Bug> { todo!() }
            }
        },
        Kind::ProcessManager,
    )
    .expect("expands")
    .to_string();
    // The kernel's start rule refuses every input for a stream with no
    // instance, of a type that starts none: the expansion routes the fact
    // and declares no starting input.
    assert!(expanded.contains(". on_fact ("), "{expanded}");
    assert!(
        expanded.contains(
            "const STARTS : & 'static [:: angzarr_embedded :: markup :: TypeName] = & [] ;"
        ),
        "{expanded}"
    );
}

/// The policy whose impl is `item`, expanded, or why not.
fn policy(item: Tokens) -> Result<String, String> {
    flow(
        quote!(name = "s", source = A, target = B),
        item,
        Flow::Policy,
    )
    .map(|expanded| expanded.to_string())
    .map_err(|error| error.to_string())
}

/// A policy's one Translate callback is its `on_refused` (decision 14.89);
/// with none, the trait's default answers every refusal a bug.
#[test]
fn a_policys_one_translate_callback_is_its_on_refused() {
    let translating = policy(quote! {
        impl S {
            #[on_refused]
            fn refused(&self, refused: &Refused) -> Result<Lost, Infallible> { todo!() }
        }
    })
    .expect("expands");
    assert!(translating.contains("fn on_refused"), "{translating}");
    assert!(
        translating.contains("< S > :: refused (self , refused)"),
        "{translating}"
    );
    let silent = policy(quote!(impl S {})).expect("expands");
    assert!(!silent.contains("fn on_refused"), "{silent}");
}

/// A second callback, one naming a command, one also an `#[on_event]`, and
/// one of another shape are refused.
#[test]
fn a_policys_translate_callback_is_one_of_its_shape() {
    let cases = [
        (
            quote! {
                impl S {
                    #[on_refused]
                    fn refused(&self, refused: &Refused) -> Result<Lost, Infallible> { todo!() }
                    #[on_refused]
                    fn again(&self, refused: &Refused) -> Result<Lost, Infallible> { todo!() }
                }
            },
            "a second Translate callback: `refused`",
        ),
        (
            quote! {
                impl S {
                    #[on_refused(On)]
                    fn refused(&self, refused: &Refused) -> Result<Lost, Infallible> { todo!() }
                }
            },
            "`#[on_refused]`, with no arguments",
        ),
        (
            quote! {
                impl S {
                    #[on_event(Rang)]
                    #[on_refused]
                    fn refused(&self, refused: &Refused) -> Result<Lost, Infallible> { todo!() }
                }
            },
            "a method takes one marker",
        ),
        (
            quote! {
                impl S {
                    #[on_refused]
                    fn refused(&self) -> Result<Lost, Infallible> { todo!() }
                }
            },
            "takes `&self` and the refusal",
        ),
        (
            quote! {
                impl S {
                    #[on_refused]
                    fn refused(&self, refused: &Refused) -> Lost { todo!() }
                }
            },
            "returns `Result<Fact, E>`",
        ),
    ];
    for (item, why) in cases {
        let error = policy(item).expect_err("refused");
        assert!(error.contains(why), "{why}: {error}");
    }
}

#[test]
fn a_flow_argument_that_is_not_key_value_is_refused() {
    let error = flow(
        quote!(name = "s", source = A, target = B, translate(On)),
        quote!(impl S {}),
        Flow::Policy,
    )
    .expect_err("refused")
    .to_string();
    assert!(
        error.contains("take `key = value` arguments only"),
        "{error}"
    );
}

#[test]
fn an_unknown_flow_argument_names_the_usage() {
    let policy = flow(
        quote!(name = "s", source = A, sink = B),
        quote!(impl S {}),
        Flow::Policy,
    )
    .expect_err("refused")
    .to_string();
    assert!(policy.contains("#[policy] takes `name"), "{policy}");
    let projector = flow(
        quote!(name = "p", target = A),
        quote!(impl P {}),
        Flow::Projector,
    )
    .expect_err("refused")
    .to_string();
    assert!(
        projector.contains("#[projector] takes `name"),
        "{projector}"
    );
}

/// A projector's sources are a list, each a domain type or a name: one
/// `source`, a bare type, an empty list and anything else in the list are
/// refused.
#[test]
fn a_projectors_sources_are_a_list_of_domains() {
    let refused = |attr: Tokens| {
        flow(attr, quote!(impl P {}), Flow::Projector)
            .expect_err("refused")
            .to_string()
    };
    for attr in [
        quote!(name = "p", source = A),
        quote!(name = "p", sources = A),
        quote!(name = "p", sources = [1]),
    ] {
        let error = refused(attr.clone());
        assert!(
            error.contains("`sources = [<domain type"),
            "{attr}: {error}"
        );
    }
    let error = refused(quote!(name = "p", sources = []));
    assert!(error.contains("at least one source domain"), "{error}");
}

/// Each source is its name: a string as written, a type as its
/// `Domain::NAME`, in order.
#[test]
fn a_projectors_sources_are_named_in_order() {
    let expanded = flow(
        quote!(name = "p", sources = [Wallet, "lamp"]),
        quote!(impl P {}),
        Flow::Projector,
    )
    .expect("expanded")
    .to_string();
    let wallet = quote!(<Wallet as ::angzarr_embedded::Domain>::NAME).to_string();
    let sources =
        format!("const SOURCES : & 'static [& 'static str] = & [{wallet} , \"lamp\" ,] ;");
    assert!(expanded.contains(&sources), "{expanded}");
}
