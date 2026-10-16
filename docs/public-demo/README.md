# QuorumLog public lab

Actual Go history checker in WebAssembly, with synthetic inputs. This page runs no Raft cluster or network fault campaign.

Build from the repository root with `bash docs/public-demo/build.sh`, then serve `docs/public-demo` with a static HTTP server. Generated Go WASM assets are ignored; use the matching Go toolchain wasm_exec.js. Publish only this directory as the Vercel project root, using its vercel.json. It contains no application API or service credentials.

Results apply only to the entered finite history. A timeout remains Unknown; an indeterminate write may or may not have taken effect.

Input limits are enforced by the local demo engine. Reset or reload discards state. CSP limits scripts, workers, and connections to this origin. No dependencies are loaded from a CDN.

After building, run `node docs/public-demo/check.cjs` from the repository root. It exercises the compiled native engine with positive and rejection controls without a server or network call.
