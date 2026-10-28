# Regression test handoff

These tests assert the intended behavior and currently fail. Production behavior is unchanged. No dependencies were installed.

Run the Rust tests from the repository root:

```sh
cargo test --workspace --offline --locked --no-fail-fast
```

Run the mocked test-harness regressions from `e2e`:

```sh
pnpm exec vitest run --config vitest.regression.config.ts
pnpm exec tsc --noEmit --strict --skipLibCheck --target ES2022 --module ESNext --moduleResolution Bundler --esModuleInterop regression/server.test.ts vitest.regression.config.ts
```

The separate Vitest configuration avoids starting the real server or invoking the existing build setup. Child processes, filesystem operations and health requests are mocked.

## Coverage

Numbers correspond to the 14 findings in the review.

| Finding | Regression coverage |
| --- | --- |
| 1. Missing presence information | `presence_snapshot_includes_members_without_user_info` |
| 2. Concurrent presence notification ordering | Deferred because deterministic coverage needs synchronization hooks around membership changes and broadcasts |
| 3. Duplicate subscriptions and identity replacement | `duplicate_presence_subscription_does_not_announce_another_join`, `replacing_presence_identity_announces_departure_before_join` |
| 4. Signed-in presence identity | `presence_without_channel_data_uses_signed_in_identity` |
| 5. Individual event size limits | `rejects_event_data_above_configured_limit`, `rejects_oversized_individual_batch_event` |
| 6. Batch body allowance | `accepts_configured_batch_count_with_valid_event_sizes` |
| 7. Missing publish fields | `rejects_missing_publish_fields`, `rejects_missing_batch_fields` |
| 8. Duplicate channel deliveries | `duplicate_channels_deliver_only_once` |
| 9. Empty string data | `outgoing_event_preserves_empty_string_data` |
| 10. HTTP admission limits | Deferred until the HTTP connection budget and overflow behavior are specified. The existing limit explicitly describes WebSocket connections |
| 11. Metrics escaping | `metrics_escapes_special_characters_in_app_keys` |
| 12. Public operational endpoints | Deployment policy decision. Decide whether access belongs in Pulse or the reverse proxy before asserting authentication behavior |
| 13. Stale E2E binary | `rebuilds the binary before testing current source` |
| 14. E2E cleanup | `finishes teardown when the server has already exited`, `cleans up the child when health checking fails` |

Identity replacement may either be rejected while preserving the original membership or announce removal before adding the replacement. Duplicate publish destinations may either be rejected with HTTP 400 or deduplicated.

The existing serialization test now checks omission of optional fields using nonempty data. This removes its previous conflicting expectation that explicitly empty event data should disappear.

## Observed results

- Rust: 33 passed and 12 failed. All 12 new tests failed at behavior assertions
- Mocked Vitest suite: all 3 new tests failed at behavior assertions
- TypeScript check for the new files: passed

Socket-based Rust tests require permission to bind local ports. An initial sandbox run failed at the existing listener test. The results above come from a rerun with socket access.
