# ADR-0333: Exact development-only picomatch audit review

Status: reviewed for the private deployment on 2026-11-18.

During #46/#47 release verification, the npm advisory database began reporting
GHSA-fc5q-39xc-vjpf / CVE-2026-112366 as HIGH. The installed picomatch is 4.0.5;
the registry's latest 4.0.7 is also affected and the advisory lists no patched
version. This finding propagates to fdir, tinyglobby, Vite and Vitest. Blindly
following npm's suggested Vite downgrade would change the toolchain without
establishing a fix.

Explicit exposure review: all affected paths in this lockfile are development
dependencies; `npm ls --omit=dev picomatch fdir tinyglobby` is empty. They process
the project's trusted build/test globs, not Agent-supplied glob patterns in an
application API. Vite is used to build static assets; it is not the deployed Web
server. Agent repositories/tools run in separate execution environments.

The owner has repeatedly selected a private-network, proportionate release policy
and cached vulnerability databases. Keep this release focused rather than add a
dependency fork or an unverified downgrade. The root audit may classify this exact
advisory as reviewed only when every affected path is development-only and the
leaf is exactly `node_modules/picomatch` version 4.0.5. Derived HIGH packages are
reviewed only if all their blocking advisory paths resolve solely to that leaf.
Unknown/cyclic evidence, a different version/path, runtime reachability or a
different HIGH/CRITICAL advisory continues to fail the gate.

Retain unfiltered findings and report reviewed/effective counts separately. This
is not a claim that the dependency is fixed and is not an image-scan exception.
Runtime artifact checks, the existing exact Cube exception and all other gates
remain unchanged. Retire this review when a verified patched dependency is adopted.
No vulnerability payload or exploit test is needed for this exposure assessment.

Sources: [upstream advisory](https://github.com/advisories/GHSA-fc5q-39xc-vjpf),
the npm registry version metadata, and this repository's pinned lockfile/production
dependency graph. At review the advisory affected versions through 4.0.7.
