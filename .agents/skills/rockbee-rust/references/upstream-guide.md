---
name: rust-skills
description: >
  Comprehensive Rust coding guidelines with 265 rules across 26 categories.
  Use when writing, reviewing, or refactoring Rust code. Covers ownership,
  error handling, async patterns, concurrency, unsafe code, API design, memory
  optimization, performance, numeric safety, conversions, serde, pattern
  matching, macros, closures, observability, testing, and common anti-patterns.
  Invoke with /rust-skills.
license: MIT
metadata:
  author: leonardomso
  version: "1.5.1"
  sources:
    - Rust API Guidelines
    - Rust Performance Book
    - Rust 2024 Edition Guide
    - The Rustonomicon
    - ripgrep, tokio, serde, polars, axum, cargo codebases
---

# Rust Best Practices

Comprehensive guide for writing high-quality, idiomatic, and highly optimized Rust code. Contains 265 rules across 26 categories, prioritized by impact to guide LLMs in code generation and refactoring. Current for Rust 1.96 (2024 edition).

## When to Apply

Reference these guidelines when:
- Writing new Rust functions, structs, or modules
- Implementing error handling or async code
- Writing concurrent, parallel, or `unsafe` code
- Designing public APIs for libraries
- Reviewing code for ownership/borrowing issues
- Optimizing memory usage or reducing allocations
- Tuning performance for hot paths
- Refactoring existing Rust code

## Rule Categories by Priority

| Priority | Category | Impact | Prefix | Rules |
|----------|----------|--------|--------|-------|
| 1 | Ownership & Borrowing | CRITICAL | `own-` | 12 |
| 2 | Error Handling | CRITICAL | `err-` | 12 |
| 3 | Memory Optimization | CRITICAL | `mem-` | 17 |
| 4 | Unsafe Code | CRITICAL | `unsafe-` | 7 |
| 5 | API Design | HIGH | `api-` | 17 |
| 6 | Async/Await | HIGH | `async-` | 18 |
| 7 | Concurrency | HIGH | `conc-` | 4 |
| 8 | Compiler Optimization | HIGH | `opt-` | 12 |
| 9 | Numeric & Arithmetic Safety | HIGH | `num-` | 5 |
| 10 | Type Safety | MEDIUM | `type-` | 13 |
| 11 | Trait & Generics Design | MEDIUM | `trait-` | 6 |
| 12 | Conversions | MEDIUM | `conv-` | 3 |
| 13 | Const & Compile-Time | MEDIUM | `const-` | 4 |
| 14 | Serde | MEDIUM | `serde-` | 8 |
| 15 | Pattern Matching | MEDIUM | `pat-` | 5 |
| 16 | Macros | MEDIUM | `macro-` | 8 |
| 17 | Closures | MEDIUM | `closure-` | 5 |
| 18 | Collections | MEDIUM | `coll-` | 4 |
| 19 | Naming Conventions | MEDIUM | `name-` | 16 |
| 20 | Testing | MEDIUM | `test-` | 15 |
| 21 | Documentation | MEDIUM | `doc-` | 12 |
| 22 | Observability | MEDIUM | `obs-` | 7 |
| 23 | Performance Patterns | MEDIUM | `perf-` | 13 |
| 24 | Project Structure | LOW | `proj-` | 14 |
| 25 | Clippy & Linting | LOW | `lint-` | 13 |
| 26 | Anti-patterns | REFERENCE | `anti-` | 15 |

---

## Quick Reference

### 1. Ownership & Borrowing (CRITICAL)

- [`own-borrow-over-clone`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-borrow-over-clone.md) - Prefer `&T` borrowing over `.clone()`
- [`own-slice-over-vec`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-slice-over-vec.md) - Accept `&[T]` not `&Vec<T>`, `&str` not `&String`
- [`own-cow-conditional`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-cow-conditional.md) - Use `Cow<'a, T>` for conditional ownership
- [`own-arc-shared`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-arc-shared.md) - Use `Arc<T>` for thread-safe shared ownership
- [`own-rc-single-thread`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-rc-single-thread.md) - Use `Rc<T>` for shared ownership in single-threaded contexts
- [`own-refcell-interior`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-refcell-interior.md) - Use `RefCell<T>` for interior mutability in single-threaded code
- [`own-mutex-interior`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-mutex-interior.md) - Use `Mutex<T>` for interior mutability across threads
- [`own-rwlock-readers`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-rwlock-readers.md) - Use `RwLock<T>` when reads significantly outnumber writes
- [`own-copy-small`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-copy-small.md) - Implement `Copy` for small, simple types
- [`own-clone-explicit`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-clone-explicit.md) - Use explicit `Clone` for types where copying has meaningful cost
- [`own-move-large`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-move-large.md) - Move large types instead of copying; use `Box` if moves are expensive
- [`own-lifetime-elision`](https://github.com/leonardomso/rust-skills/blob/master/rules/own-lifetime-elision.md) - Rely on lifetime elision rules; add explicit lifetimes only when required

### 2. Error Handling (CRITICAL)

- [`err-thiserror-lib`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-thiserror-lib.md) - Use `thiserror` for library error types
- [`err-anyhow-app`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-anyhow-app.md) - Use `anyhow` for application error handling
- [`err-result-over-panic`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-result-over-panic.md) - Return `Result<T, E>` instead of panicking for recoverable errors
- [`err-context-chain`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-context-chain.md) - Add context with `.context()` or `.with_context()`
- [`err-no-unwrap-prod`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-no-unwrap-prod.md) - Avoid `unwrap()` in production code; use `?`, `expect()`, or handle errors
- [`err-expect-bugs-only`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-expect-bugs-only.md) - Use `expect()` only for invariants that indicate bugs, not user errors
- [`err-question-mark`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-question-mark.md) - Use `?` operator for clean propagation
- [`err-from-impl`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-from-impl.md) - Implement `From<E>` for error conversions to enable `?` operator
- [`err-source-chain`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-source-chain.md) - Preserve error chains with `#[source]` or `source()` method
- [`err-lowercase-msg`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-lowercase-msg.md) - Start error messages lowercase, no trailing punctuation
- [`err-doc-errors`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-doc-errors.md) - Document error conditions with `# Errors` section in doc comments
- [`err-custom-type`](https://github.com/leonardomso/rust-skills/blob/master/rules/err-custom-type.md) - Define custom error types for domain-specific failures

### 3. Memory Optimization (CRITICAL)

- [`mem-with-capacity`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-with-capacity.md) - Use `with_capacity()` when size is known
- [`mem-smallvec`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-smallvec.md) - Use `SmallVec` for usually-small collections
- [`mem-arrayvec`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-arrayvec.md) - Use `ArrayVec<T, N>` for fixed-capacity collections that never heap-allocate
- [`mem-box-large-variant`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-box-large-variant.md) - Box large enum variants to reduce overall enum size
- [`mem-boxed-slice`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-boxed-slice.md) - Use `Box<[T]>` instead of `Vec<T>` for fixed-size heap data
- [`mem-thinvec`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-thinvec.md) - Use `ThinVec<T>` for nullable collections with minimal overhead
- [`mem-clone-from`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-clone-from.md) - Use `clone_from()` to reuse allocations when repeatedly cloning
- [`mem-reuse-collections`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-reuse-collections.md) - Clear and reuse collections instead of creating new ones in loops
- [`mem-avoid-format`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-avoid-format.md) - Avoid `format!()` when string literals work
- [`mem-write-over-format`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-write-over-format.md) - Use `write!()` into existing buffers instead of `format!()` allocations
- [`mem-arena-allocator`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-arena-allocator.md) - Use arena allocators for batch allocations
- [`mem-zero-copy`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-zero-copy.md) - Use zero-copy patterns with slices and `Bytes`
- [`mem-compact-string`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-compact-string.md) - Use compact string types for memory-constrained string storage
- [`mem-smaller-integers`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-smaller-integers.md) - Use appropriately-sized integers to reduce memory footprint
- [`mem-assert-type-size`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-assert-type-size.md) - Use static assertions to guard against accidental type size growth
- [`mem-take-replace`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-take-replace.md) - Use `mem::take` / `mem::replace` to move a value out of a `&mut` without cloning
- [`mem-drop-order`](https://github.com/leonardomso/rust-skills/blob/master/rules/mem-drop-order.md) - Know and control drop order: struct fields drop top-to-bottom, locals in reverse

### 4. Unsafe Code (CRITICAL)

- [`unsafe-safety-comment`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-safety-comment.md) - Write a `// SAFETY:` comment above every `unsafe` block and a `# Safety` section in every `unsafe fn`.
- [`unsafe-minimize-scope`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-minimize-scope.md) - Keep `unsafe` blocks as small as possible — mark only the operation that requires unsafety, not the surrounding safe code.
- [`unsafe-miri-ci`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-miri-ci.md) - Run `cargo miri test` in CI for every crate that contains `unsafe` code.
- [`unsafe-maybeuninit`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-maybeuninit.md) - Use `MaybeUninit<T>` for uninitialized memory; never use `mem::uninitialized()` or `mem::zeroed()` for types with validity invariants.
- [`unsafe-extern-block`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-extern-block.md) - In Rust 2024, wrap `extern` blocks in `unsafe extern { }` and annotate each item as `safe` or `unsafe`.
- [`unsafe-send-sync-manual`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-send-sync-manual.md) - Document the invariants when manually implementing `Send` or `Sync`; prefer letting the compiler derive them automatically.
- [`unsafe-no-mangle-unsafe`](https://github.com/leonardomso/rust-skills/blob/master/rules/unsafe-no-mangle-unsafe.md) - In Rust 2024, write `#[unsafe(no_mangle)]`, `#[unsafe(export_name = "...")]`, and `#[unsafe(link_section = "...")]` — not the bare attribute forms.

### 5. API Design (HIGH)

- [`api-builder-pattern`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-builder-pattern.md) - Use Builder pattern for complex construction
- [`api-builder-must-use`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-builder-must-use.md) - Mark builder methods with `#[must_use]` to prevent silent drops
- [`api-newtype-safety`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-newtype-safety.md) - Use newtypes to prevent mixing semantically different values
- [`api-typestate`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-typestate.md) - Use typestate pattern to encode state machine invariants in the type system
- [`api-sealed-trait`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-sealed-trait.md) - Use sealed traits to prevent external implementations while allowing use
- [`api-extension-trait`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-extension-trait.md) - Use extension traits to add methods to external types
- [`api-parse-dont-validate`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-parse-dont-validate.md) - Parse into validated types at boundaries
- [`api-impl-into`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-impl-into.md) - Accept `impl Into<T>` for flexible APIs, implement `From<T>` for conversions
- [`api-impl-asref`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-impl-asref.md) - Use `AsRef<T>` when you only need to borrow the inner data
- [`api-must-use`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-must-use.md) - Mark types and functions with `#[must_use]` when ignoring results is likely a bug
- [`api-non-exhaustive`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-non-exhaustive.md) - Use `#[non_exhaustive]` on public enums and structs for forward compatibility
- [`api-from-not-into`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-from-not-into.md) - Implement `From<T>`, not `Into<U>` - From gives you Into for free
- [`api-default-impl`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-default-impl.md) - Implement `Default` for types with sensible default values
- [`api-common-traits`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-common-traits.md) - Implement standard traits (Debug, Clone, PartialEq, etc.) for public types
- [`api-serde-optional`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-serde-optional.md) - Make serde a feature flag, not a hard dependency for library crates
- [`api-impl-fromiterator`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-impl-fromiterator.md) - Implement `FromIterator` and `Extend` for collection types, and `IntoIterator` for all three reference forms
- [`api-operator-overload`](https://github.com/leonardomso/rust-skills/blob/master/rules/api-operator-overload.md) - Overload operators only when the semantics are natural and unsurprising

### 6. Async/Await (HIGH)

- [`async-tokio-runtime`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-tokio-runtime.md) - Configure Tokio runtime appropriately for your workload
- [`async-no-lock-await`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-no-lock-await.md) - Never hold `Mutex`/`RwLock` across `.await`
- [`async-spawn-blocking`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-spawn-blocking.md) - Use `spawn_blocking` for CPU-intensive work
- [`async-tokio-fs`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-tokio-fs.md) - Use `tokio::fs` instead of `std::fs` in async code
- [`async-cancellation-token`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-cancellation-token.md) - Use `CancellationToken` for graceful shutdown and task cancellation
- [`async-join-parallel`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-join-parallel.md) - Use `join!` or `try_join!` for concurrent independent futures
- [`async-try-join`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-try-join.md) - Use `try_join!` for concurrent fallible operations with early return on error
- [`async-select-racing`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-select-racing.md) - Use `select!` to race futures and handle the first to complete
- [`async-bounded-channel`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-bounded-channel.md) - Use bounded channels to apply backpressure and prevent unbounded memory growth
- [`async-mpsc-queue`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-mpsc-queue.md) - Use `mpsc` channels for async message queues between tasks
- [`async-broadcast-pubsub`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-broadcast-pubsub.md) - Use `broadcast` channel for pub/sub where all subscribers receive all messages
- [`async-watch-latest`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-watch-latest.md) - Use `watch` channel for sharing the latest value with multiple observers
- [`async-oneshot-response`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-oneshot-response.md) - Use `oneshot` channel for request-response patterns
- [`async-joinset-structured`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-joinset-structured.md) - Use `JoinSet` for managing dynamic collections of spawned tasks
- [`async-clone-before-await`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-clone-before-await.md) - Clone Arc/Rc data before await points to avoid holding references across suspension
- [`async-fn-in-trait`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-fn-in-trait.md) - Use native `async fn` in traits (stable 1.75) instead of the `async_trait` macro
- [`async-async-fn-bounds`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-async-fn-bounds.md) - Use `AsyncFn`/`AsyncFnMut`/`AsyncFnOnce` bounds instead of `F: Fn() -> Fut, Fut: Future`
- [`async-cancel-safety`](https://github.com/leonardomso/rust-skills/blob/master/rules/async-cancel-safety.md) - Ensure futures used in `tokio::select!` branches are cancellation-safe

### 7. Concurrency (HIGH)

- [`conc-rayon-par-iter`](https://github.com/leonardomso/rust-skills/blob/master/rules/conc-rayon-par-iter.md) - Use rayon's `par_iter()` for CPU-bound data parallelism
- [`conc-scoped-threads`](https://github.com/leonardomso/rust-skills/blob/master/rules/conc-scoped-threads.md) - Use `std::thread::scope` to borrow stack data across threads
- [`conc-atomic-ordering`](https://github.com/leonardomso/rust-skills/blob/master/rules/conc-atomic-ordering.md) - Use the weakest correct memory `Ordering` for every atomic operation
- [`conc-thread-local`](https://github.com/leonardomso/rust-skills/blob/master/rules/conc-thread-local.md) - Prefer `thread_local!` with `Cell`/`RefCell` over `static mut`

### 8. Compiler Optimization (HIGH)

- [`opt-inline-small`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-inline-small.md) - Use `#[inline]` for small hot functions
- [`opt-inline-always-rare`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-inline-always-rare.md) - Use `#[inline(always)]` sparingly—only for critical hot paths proven by profiling
- [`opt-inline-never-cold`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-inline-never-cold.md) - Use `#[inline(never)]` and `#[cold]` for error paths and rarely-executed code
- [`opt-cold-unlikely`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-cold-unlikely.md) - Mark unlikely code paths with `#[cold]` to help compiler optimization
- [`opt-likely-hint`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-likely-hint.md) - Use code structure to hint at likely branches; use intrinsics on nightly
- [`opt-lto-release`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-lto-release.md) - Enable LTO in release builds
- [`opt-codegen-units`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-codegen-units.md) - Set `codegen-units = 1` for maximum optimization in release builds
- [`opt-pgo-profile`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-pgo-profile.md) - Use Profile-Guided Optimization (PGO) for maximum performance
- [`opt-target-cpu`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-target-cpu.md) - Use `target-cpu=native` for maximum performance on known deployment targets
- [`opt-bounds-check`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-bounds-check.md) - Use iterators and patterns that eliminate bounds checks in hot paths
- [`opt-simd-portable`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-simd-portable.md) - Use portable SIMD for vectorized operations across architectures
- [`opt-cache-friendly`](https://github.com/leonardomso/rust-skills/blob/master/rules/opt-cache-friendly.md) - Organize data for cache-efficient access patterns

### 9. Numeric & Arithmetic Safety (HIGH)

- [`num-overflow-explicit`](https://github.com/leonardomso/rust-skills/blob/master/rules/num-overflow-explicit.md) - Handle integer overflow explicitly: `checked_`/`saturating_`/`wrapping_`/`overflowing_`
- [`num-cast-try-from`](https://github.com/leonardomso/rust-skills/blob/master/rules/num-cast-try-from.md) - Avoid `as` for narrowing casts; use `From` for widening and `TryFrom` for narrowing
- [`num-float-compare`](https://github.com/leonardomso/rust-skills/blob/master/rules/num-float-compare.md) - Don't compare floats with `==`; use a tolerance, and `total_cmp` for ordering
- [`num-saturating-clamp`](https://github.com/leonardomso/rust-skills/blob/master/rules/num-saturating-clamp.md) - Bound values with `clamp` and saturating arithmetic
- [`num-nonzero`](https://github.com/leonardomso/rust-skills/blob/master/rules/num-nonzero.md) - Use `NonZero*` types to forbid zero and unlock the niche optimization

### 10. Type Safety (MEDIUM)

- [`type-newtype-ids`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-newtype-ids.md) - Wrap IDs in newtypes: `UserId(u64)`
- [`type-newtype-validated`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-newtype-validated.md) - Use newtypes to enforce validation at construction time
- [`type-enum-states`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-enum-states.md) - Use enums for mutually exclusive states
- [`type-option-nullable`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-option-nullable.md) - Use `Option<T>` for values that might not exist
- [`type-result-fallible`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-result-fallible.md) - Use `Result<T, E>` for operations that can fail
- [`type-phantom-marker`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-phantom-marker.md) - Use `PhantomData` to express type relationships without runtime cost
- [`type-never-diverge`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-never-diverge.md) - Use `!` (never type) for functions that never return
- [`type-generic-bounds`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-generic-bounds.md) - Add trait bounds only where needed, prefer where clauses for readability
- [`type-no-stringly`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-no-stringly.md) - Avoid stringly-typed APIs; use enums, newtypes, or validated types
- [`type-repr-transparent`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-repr-transparent.md) - Use `#[repr(transparent)]` for newtypes in FFI contexts
- [`type-deref-coercion`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-deref-coercion.md) - Implement `Deref`/`DerefMut` only for smart-pointer and transparent wrapper types
- [`type-display-vs-debug`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-display-vs-debug.md) - Use `Display` for user-facing output and `Debug` for diagnostics; never swap them
- [`type-numeric-fmt`](https://github.com/leonardomso/rust-skills/blob/master/rules/type-numeric-fmt.md) - Implement `LowerHex`, `UpperHex`, `Octal`, and `Binary` for numeric newtypes

### 11. Trait & Generics Design (MEDIUM)

- [`trait-associated-type-vs-generic`](https://github.com/leonardomso/rust-skills/blob/master/rules/trait-associated-type-vs-generic.md) - Use an associated type when each impl has exactly one output type; use a generic parameter when a type can implement the trait for many input types
- [`trait-blanket-impl`](https://github.com/leonardomso/rust-skills/blob/master/rules/trait-blanket-impl.md) - Use a blanket impl `impl<T: Bound> Trait for T` to give behaviour to every type that satisfies a bound
- [`trait-coherence-newtype`](https://github.com/leonardomso/rust-skills/blob/master/rules/trait-coherence-newtype.md) - Respect the orphan rule; wrap a foreign type in a newtype to implement a foreign trait on it
- [`trait-default-methods`](https://github.com/leonardomso/rust-skills/blob/master/rules/trait-default-methods.md) - Define a trait in terms of a few required methods plus defaulted ones built on top of them
- [`trait-dyn-vs-generic`](https://github.com/leonardomso/rust-skills/blob/master/rules/trait-dyn-vs-generic.md) - Choose static dispatch (generics / `impl Trait`) vs dynamic dispatch (`dyn Trait`) deliberately
- [`trait-object-safety`](https://github.com/leonardomso/rust-skills/blob/master/rules/trait-object-safety.md) - Keep a trait dyn-compatible (object-safe) when you need `dyn Trait`

### 12. Conversions (MEDIUM)

- [`conv-tryfrom-fallible`](https://github.com/leonardomso/rust-skills/blob/master/rules/conv-tryfrom-fallible.md) - Implement `TryFrom` for fallible conversions instead of ad-hoc conversion functions
- [`conv-fromstr-parsing`](https://github.com/leonardomso/rust-skills/blob/master/rules/conv-fromstr-parsing.md) - Implement `FromStr` to enable `str::parse` for string-to-type conversions
- [`conv-asmut-mutable`](https://github.com/leonardomso/rust-skills/blob/master/rules/conv-asmut-mutable.md) - Accept `impl AsMut<T>` for flexible mutable borrowed inputs instead of concrete mutable references

### 13. Const & Compile-Time (MEDIUM)

- [`const-block`](https://github.com/leonardomso/rust-skills/blob/master/rules/const-block.md) - Use inline `const { }` blocks for compile-time evaluation and assertions
- [`const-fn`](https://github.com/leonardomso/rust-skills/blob/master/rules/const-fn.md) - Make functions `const fn` when they can run at compile time
- [`const-generics`](https://github.com/leonardomso/rust-skills/blob/master/rules/const-generics.md) - Parameterize over values with const generics `<const N: usize>`
- [`const-vs-static`](https://github.com/leonardomso/rust-skills/blob/master/rules/const-vs-static.md) - Use `const` for an inlined value and `static` for a single addressed instance

### 14. Serde (MEDIUM)

- [`serde-rename-all`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-rename-all.md) - Match the external naming convention with `#[serde(rename_all = ...)]`
- [`serde-default-compat`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-default-compat.md) - Use `#[serde(default)]` for optional and backward-compatible fields
- [`serde-skip-empty`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-skip-empty.md) - Omit empty fields with `skip_serializing_if`
- [`serde-flatten`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-flatten.md) - Inline nested structs or capture extra keys with `#[serde(flatten)]`
- [`serde-enum-representation`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-enum-representation.md) - Choose enum tagging deliberately: externally, internally, adjacently tagged, or untagged
- [`serde-deny-unknown-fields`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-deny-unknown-fields.md) - Reject unexpected keys with `#[serde(deny_unknown_fields)]`
- [`serde-custom-with`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-custom-with.md) - Customize a field's (de)serialization with `with` / `serialize_with` / `deserialize_with`
- [`serde-try-from-validate`](https://github.com/leonardomso/rust-skills/blob/master/rules/serde-try-from-validate.md) - Validate while deserializing with `#[serde(try_from = "Raw")]`

### 15. Pattern Matching (MEDIUM)

- [`pat-let-else`](https://github.com/leonardomso/rust-skills/blob/master/rules/pat-let-else.md) - Use `let ... else` for early-return pattern extraction
- [`pat-matches-macro`](https://github.com/leonardomso/rust-skills/blob/master/rules/pat-matches-macro.md) - Use `matches!()` for boolean pattern tests
- [`pat-if-let-chains`](https://github.com/leonardomso/rust-skills/blob/master/rules/pat-if-let-chains.md) - Use `if let` chains to combine pattern bindings and conditions
- [`pat-exhaustive-enum`](https://github.com/leonardomso/rust-skills/blob/master/rules/pat-exhaustive-enum.md) - Match owned enums exhaustively; avoid catch-all `_` that hides new variants
- [`pat-at-bindings`](https://github.com/leonardomso/rust-skills/blob/master/rules/pat-at-bindings.md) - Use `@` bindings to capture a value while matching it against a pattern

### 16. Macros (MEDIUM)

- [`macro-prefer-functions`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-prefer-functions.md) - Reach for a macro only when a function or generic cannot express it
- [`macro-rules-hygiene`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-rules-hygiene.md) - Rely on `macro_rules!` hygiene and use `$crate` for paths to your crate's items
- [`macro-fragment-specifiers`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-fragment-specifiers.md) - Capture with precise fragment specifiers, not raw `:tt`, where you can
- [`macro-export-crate-path`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-export-crate-path.md) - Export declarative macros with `#[macro_export]` and a clean import path
- [`macro-private-helpers`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-private-helpers.md) - Hide macro-generated helper items behind a `#[doc(hidden)] pub mod __private`
- [`macro-proc-two-crate`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-proc-two-crate.md) - Put procedural macros in a dedicated `proc-macro = true` crate and re-export from the facade
- [`macro-proc-syn-quote`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-proc-syn-quote.md) - Build procedural macros with `syn`, `quote`, and `proc-macro2`
- [`macro-proc-error-spans`](https://github.com/leonardomso/rust-skills/blob/master/rules/macro-proc-error-spans.md) - Report proc-macro errors as spanned compile errors, never by panicking

### 17. Closures (MEDIUM)

- [`closure-fn-trait-bounds`](https://github.com/leonardomso/rust-skills/blob/master/rules/closure-fn-trait-bounds.md) - Require the least restrictive `Fn` trait a callback needs (`FnOnce` ⊇ `FnMut` ⊇ `Fn`)
- [`closure-impl-fn-return`](https://github.com/leonardomso/rust-skills/blob/master/rules/closure-impl-fn-return.md) - Return closures as `impl Fn`/`FnMut`/`FnOnce`, not `Box<dyn Fn>`
- [`closure-move-capture`](https://github.com/leonardomso/rust-skills/blob/master/rules/closure-move-capture.md) - Use `move` for closures that outlive the current scope; clone before `move` to keep the original
- [`closure-static-vs-dyn`](https://github.com/leonardomso/rust-skills/blob/master/rules/closure-static-vs-dyn.md) - Accept `impl Fn` (generic) for hot callbacks; use `&dyn Fn`/`Box<dyn Fn>` to cut code size or to store them
- [`closure-disjoint-capture`](https://github.com/leonardomso/rust-skills/blob/master/rules/closure-disjoint-capture.md) - Capture only what you use; lean on edition-2021 disjoint closure captures

### 18. Collections (MEDIUM)

- [`coll-binaryheap`](https://github.com/leonardomso/rust-skills/blob/master/rules/coll-binaryheap.md) - Use `BinaryHeap` for a priority queue or repeated max-extraction
- [`coll-map-choice`](https://github.com/leonardomso/rust-skills/blob/master/rules/coll-map-choice.md) - Pick the map by access pattern: `HashMap` (fast, unordered), `BTreeMap` (sorted / range queries), `IndexMap` (insertion order)
- [`coll-seq-choice`](https://github.com/leonardomso/rust-skills/blob/master/rules/coll-seq-choice.md) - Default to `Vec`; use `VecDeque` for queue/deque behaviour; avoid `LinkedList`
- [`coll-set-membership`](https://github.com/leonardomso/rust-skills/blob/master/rules/coll-set-membership.md) - Use `HashSet`/`BTreeSet` for membership tests and dedup, not linear `Vec::contains`

### 19. Naming Conventions (MEDIUM)

- [`name-types-camel`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-types-camel.md) - Use `UpperCamelCase` for types, traits, and enum names
- [`name-variants-camel`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-variants-camel.md) - Use `UpperCamelCase` for enum variants
- [`name-funcs-snake`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-funcs-snake.md) - Use `snake_case` for functions, methods, variables, and modules
- [`name-consts-screaming`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-consts-screaming.md) - Use `SCREAMING_SNAKE_CASE` for constants and statics
- [`name-lifetime-short`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-lifetime-short.md) - Use short, conventional lifetime names: `'a`, `'b`, `'de`, `'src`
- [`name-type-param-single`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-type-param-single.md) - Use single uppercase letters for type parameters: `T`, `E`, `K`, `V`
- [`name-as-free`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-as-free.md) - `as_` prefix: free reference conversion
- [`name-to-expensive`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-to-expensive.md) - Use `to_` prefix for expensive conversions that allocate or compute
- [`name-into-ownership`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-into-ownership.md) - Use `into_` prefix for ownership-consuming conversions
- [`name-no-get-prefix`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-no-get-prefix.md) - Omit get_ prefix for simple getters
- [`name-is-has-bool`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-is-has-bool.md) - Use `is_`, `has_`, `can_`, `should_` prefixes for boolean-returning methods
- [`name-iter-convention`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-iter-convention.md) - Use iter/iter_mut/into_iter for iterator methods
- [`name-iter-method`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-iter-method.md) - Name iterator methods `iter()`, `iter_mut()`, and `into_iter()` consistently
- [`name-iter-type-match`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-iter-type-match.md) - Name iterator types after their source method
- [`name-acronym-word`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-acronym-word.md) - Treat acronyms as words in identifiers: `HttpServer`, not `HTTPServer`
- [`name-crate-no-rs`](https://github.com/leonardomso/rust-skills/blob/master/rules/name-crate-no-rs.md) - Don't suffix crate names with `-rs` or `-rust`

### 20. Testing (MEDIUM)

- [`test-cfg-test-module`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-cfg-test-module.md) - Put unit tests in `#[cfg(test)] mod tests { }` within each module
- [`test-use-super`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-use-super.md) - Use `use super::*;` in test modules to access parent module items
- [`test-integration-dir`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-integration-dir.md) - Put integration tests in the `tests/` directory
- [`test-descriptive-names`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-descriptive-names.md) - Use descriptive test names that explain what is being tested
- [`test-arrange-act-assert`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-arrange-act-assert.md) - Structure tests with clear Arrange, Act, Assert sections
- [`test-proptest-properties`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-proptest-properties.md) - Use proptest for property-based testing
- [`test-mockall-mocking`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-mockall-mocking.md) - Use mockall for trait mocking
- [`test-mock-traits`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-mock-traits.md) - Use traits for dependencies to enable mocking in tests
- [`test-fixture-raii`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-fixture-raii.md) - Use RAII pattern (Drop trait) for automatic test cleanup
- [`test-tokio-async`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-tokio-async.md) - Use `#[tokio::test]` for async tests
- [`test-should-panic`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-should-panic.md) - Use `#[should_panic]` to test that code panics as expected
- [`test-criterion-bench`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-criterion-bench.md) - Use `criterion` for benchmarking
- [`test-doctest-examples`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-doctest-examples.md) - Keep documentation examples as executable doctests
- [`test-loom-concurrency`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-loom-concurrency.md) - Use `loom` to exhaustively test lock-free and concurrent code
- [`test-snapshot-testing`](https://github.com/leonardomso/rust-skills/blob/master/rules/test-snapshot-testing.md) - Use snapshot testing (insta) for complex or serialized output

### 21. Documentation (MEDIUM)

- [`doc-all-public`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-all-public.md) - Document all public items with `///` doc comments
- [`doc-module-inner`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-module-inner.md) - Use `//!` for module-level documentation
- [`doc-examples-section`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-examples-section.md) - Include `# Examples` with runnable code
- [`doc-errors-section`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-errors-section.md) - Include `# Errors` section for fallible functions
- [`doc-panics-section`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-panics-section.md) - Include `# Panics` section for functions that can panic
- [`doc-safety-section`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-safety-section.md) - Include `# Safety` section for unsafe functions
- [`doc-question-mark`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-question-mark.md) - Use `?` in examples, not `.unwrap()`
- [`doc-hidden-setup`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-hidden-setup.md) - Use `# ` prefix to hide example setup code
- [`doc-intra-links`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-intra-links.md) - Use intra-doc links to reference types and items
- [`doc-link-types`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-link-types.md) - Use intra-doc links to connect related types and functions
- [`doc-cargo-metadata`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-cargo-metadata.md) - Fill `Cargo.toml` metadata for published crates
- [`doc-crate-readme`](https://github.com/leonardomso/rust-skills/blob/master/rules/doc-crate-readme.md) - Unify the README and crate root docs with `#![doc = include_str!("../README.md")]`

### 22. Observability (MEDIUM)

- [`obs-tracing-over-log`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-tracing-over-log.md) - Use `tracing` for structured, span-aware diagnostics instead of `println!` or bare `log`
- [`obs-library-facade`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-library-facade.md) - Libraries emit through the tracing/log facade and never install a subscriber
- [`obs-structured-fields`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-structured-fields.md) - Record structured key-value fields, not values interpolated into the message string
- [`obs-instrument-spans`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-instrument-spans.md) - Use `#[tracing::instrument]` and spans to attach context to async tasks and requests
- [`obs-levels-filter`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-levels-filter.md) - Use log levels meaningfully and filter with `EnvFilter` / `RUST_LOG`
- [`obs-error-chain`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-error-chain.md) - Log errors with their full source chain, and log each error exactly once
- [`obs-no-sensitive-data`](https://github.com/leonardomso/rust-skills/blob/master/rules/obs-no-sensitive-data.md) - Never log secrets or PII; redact or skip them

### 23. Performance Patterns (MEDIUM)

- [`perf-iter-over-index`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-iter-over-index.md) - Prefer iterators over manual indexing
- [`perf-iter-lazy`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-iter-lazy.md) - Keep iterators lazy, collect only when needed
- [`perf-collect-once`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-collect-once.md) - Don't collect intermediate iterators
- [`perf-entry-api`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-entry-api.md) - Use entry API for map insert-or-update
- [`perf-drain-reuse`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-drain-reuse.md) - Use drain to reuse allocations
- [`perf-extend-batch`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-extend-batch.md) - Use extend for batch insertions
- [`perf-chain-avoid`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-chain-avoid.md) - Avoid chain in hot loops
- [`perf-collect-into`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-collect-into.md) - Use collect_into for reusing containers
- [`perf-black-box-bench`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-black-box-bench.md) - Use black_box in benchmarks
- [`perf-release-profile`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-release-profile.md) - Optimize release profile settings
- [`perf-profile-first`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-profile-first.md) - Profile before optimizing
- [`perf-ahash`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-ahash.md) - Use a faster hasher (`ahash` / `FxHashMap`) when DoS resistance is not needed
- [`perf-io-buffering`](https://github.com/leonardomso/rust-skills/blob/master/rules/perf-io-buffering.md) - Wrap `Read`/`Write` in `BufReader`/`BufWriter` for many small operations

### 24. Project Structure (LOW)

- [`proj-lib-main-split`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-lib-main-split.md) - Keep `main.rs` minimal, logic in `lib.rs`
- [`proj-mod-by-feature`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-mod-by-feature.md) - Organize modules by feature, not type
- [`proj-flat-small`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-flat-small.md) - Keep small projects flat
- [`proj-mod-rs-dir`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-mod-rs-dir.md) - Use mod.rs for multi-file modules
- [`proj-pub-crate-internal`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-pub-crate-internal.md) - Use pub(crate) for internal APIs
- [`proj-pub-super-parent`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-pub-super-parent.md) - Use pub(super) for parent-only visibility
- [`proj-pub-use-reexport`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-pub-use-reexport.md) - Use pub use for clean public API
- [`proj-prelude-module`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-prelude-module.md) - Create prelude module for common imports
- [`proj-bin-dir`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-bin-dir.md) - Put multiple binaries in src/bin/
- [`proj-workspace-large`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-workspace-large.md) - Use workspaces for large projects
- [`proj-workspace-deps`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-workspace-deps.md) - Use workspace dependency inheritance for consistent versions across crates
- [`proj-feature-additive`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-feature-additive.md) - Design Cargo features to be strictly additive
- [`proj-msrv-declare`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-msrv-declare.md) - Declare `rust-version` (MSRV) in Cargo.toml and test it in CI
- [`proj-build-rs-minimal`](https://github.com/leonardomso/rust-skills/blob/master/rules/proj-build-rs-minimal.md) - Keep `build.rs` minimal, deterministic, and idempotent

### 25. Clippy & Linting (LOW)

- [`lint-deny-correctness`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-deny-correctness.md) - `#![deny(clippy::correctness)]`
- [`lint-warn-suspicious`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-warn-suspicious.md) - Enable clippy::suspicious for likely bugs
- [`lint-warn-style`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-warn-style.md) - Enable clippy::style for idiomatic code
- [`lint-warn-complexity`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-warn-complexity.md) - Enable clippy::complexity for simpler code
- [`lint-warn-perf`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-warn-perf.md) - Enable clippy::perf for performance improvements
- [`lint-pedantic-selective`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-pedantic-selective.md) - Enable clippy::pedantic selectively
- [`lint-missing-docs`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-missing-docs.md) - Warn on missing documentation for public items
- [`lint-unsafe-doc`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-unsafe-doc.md) - Require documentation for unsafe blocks
- [`lint-cargo-metadata`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-cargo-metadata.md) - Enable clippy::cargo for published crates
- [`lint-rustfmt-check`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-rustfmt-check.md) - Run cargo fmt --check in CI
- [`lint-workspace-lints`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-workspace-lints.md) - Configure lints at workspace level for consistent enforcement
- [`lint-cfg-check`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-cfg-check.md) - Enable `unexpected_cfgs` and declare known cfgs to catch feature-gate typos
- [`lint-clippy-nursery-selected`](https://github.com/leonardomso/rust-skills/blob/master/rules/lint-clippy-nursery-selected.md) - Enable high-value `clippy::nursery` lints selectively, not the whole group

### 26. Anti-patterns (REFERENCE)

- [`anti-unwrap-abuse`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-unwrap-abuse.md) - Don't use `.unwrap()` in production code
- [`anti-expect-lazy`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-expect-lazy.md) - Don't use expect for recoverable errors
- [`anti-clone-excessive`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-clone-excessive.md) - Don't clone when borrowing works
- [`anti-lock-across-await`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-lock-across-await.md) - Don't hold locks across await points
- [`anti-string-for-str`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-string-for-str.md) - Don't accept &String when &str works
- [`anti-vec-for-slice`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-vec-for-slice.md) - Don't accept &Vec<T> when &[T] works
- [`anti-index-over-iter`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-index-over-iter.md) - Don't use indexing when iterators work
- [`anti-panic-expected`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-panic-expected.md) - Don't panic on expected or recoverable errors
- [`anti-empty-catch`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-empty-catch.md) - Don't silently ignore errors
- [`anti-over-abstraction`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-over-abstraction.md) - Don't over-abstract with excessive generics
- [`anti-premature-optimize`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-premature-optimize.md) - Don't optimize before profiling
- [`anti-type-erasure`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-type-erasure.md) - Don't use Box<dyn Trait> when impl Trait works
- [`anti-format-hot-path`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-format-hot-path.md) - Don't use format! in hot paths
- [`anti-collect-intermediate`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-collect-intermediate.md) - Don't collect intermediate iterators
- [`anti-stringly-typed`](https://github.com/leonardomso/rust-skills/blob/master/rules/anti-stringly-typed.md) - Don't use strings where enums or newtypes would provide type safety

---

## Recommended Cargo.toml Settings

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true

[profile.bench]
inherits = "release"
debug = true
strip = false

[profile.dev]
opt-level = 0
debug = true

[profile.dev.package."*"]
opt-level = 3  # Optimize dependencies in dev
```

---

## How to Use

This skill provides rule identifiers for quick reference. When generating or reviewing Rust code:

1. **Check relevant category** based on task type
2. **Apply rules** with matching prefix
3. **Prioritize** CRITICAL > HIGH > MEDIUM > LOW
4. **Read rule files** in `rules/` for detailed examples

### Rule Application by Task

| Task | Primary Categories |
|------|-------------------|
| New function | `own-`, `err-`, `name-`, `pat-` |
| New struct/API | `api-`, `type-`, `conv-`, `doc-` |
| Async code | `async-`, `own-` |
| Concurrency / parallelism | `conc-`, `async-`, `own-` |
| Unsafe code | `unsafe-`, `type-`, `test-` |
| Error handling | `err-`, `api-`, `pat-` |
| Type conversions | `conv-`, `api-` |
| Serialization (serde) | `serde-`, `type-`, `api-` |
| Numeric / arithmetic | `num-`, `type-` |
| Macros / code generation | `macro-`, `anti-` |
| Closures / callbacks | `closure-`, `type-` |
| Logging / observability | `obs-`, `err-` |
| Memory optimization | `mem-`, `own-`, `perf-` |
| Performance tuning | `opt-`, `mem-`, `perf-` |
| Code review | `anti-`, `lint-` |

---

## Sources & Attribution

This skill is an independent synthesis of official Rust guidance, well-known books, and patterns from widely-used crates. It is not affiliated with or endorsed by the Rust project or any crate author; the text and code examples are original.

**Official Rust documentation**
- [The Rust Reference](https://doc.rust-lang.org/reference/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [The Rustonomicon](https://doc.rust-lang.org/nomicon/) (unsafe code)
- [Rust 2024 Edition Guide](https://doc.rust-lang.org/edition-guide/rust-2024/)
- [The Cargo Book](https://doc.rust-lang.org/cargo/)
- [Standard library docs](https://doc.rust-lang.org/std/) and [release notes](https://doc.rust-lang.org/releases.html)

**Books & guides**
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/) — Nicholas Nethercote
- [Rust Design Patterns](https://rust-unofficial.github.io/patterns/) — rust-unofficial
- [Rust Atomics and Locks](https://marabos.nl/atomics/) — Mara Bos
- [Effective Rust](https://effective-rust.com/) — David Drysdale

**Tooling**
- [Clippy lint documentation](https://rust-lang.github.io/rust-clippy/)
- [Miri](https://github.com/rust-lang/miri)

**Real-world codebases studied for idioms**
- ripgrep, tokio, serde, clap, polars, axum, cargo, hyper, bevy, rayon, and dtolnay's crates (thiserror, anyhow, syn)

This project is MIT-licensed. Referenced upstream materials remain under their own licenses (the official Rust docs and API Guidelines are dual MIT / Apache-2.0).
