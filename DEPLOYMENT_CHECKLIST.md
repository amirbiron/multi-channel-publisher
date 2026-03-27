# Deployment Checklist — Multi-Channel Publisher (GBP Rollout)

## Pre-Deployment

### Environment Variables
- [ ] `GBP_ACCOUNT_ID` set (e.g. `accounts/123456789`)
- [ ] `GBP_OAUTH_CLIENT_ID` set
- [ ] `GBP_OAUTH_CLIENT_SECRET` set
- [ ] `GBP_REFRESH_TOKEN` set and valid
- [ ] `GBP_ENABLED` set to `false` (feature flag — start disabled)
- [ ] Google Cloud project approved for GBP API access
- [ ] OAuth scopes include `business.manage`

### Google Sheets
- [ ] New columns added to sheet: `caption`, `caption_gbp`, `gbp_post_type`, `cta_type`, `cta_url`, `google_location_id`, `source`, `locked_at`, `processing_by`, `retry_count`, `published_channels`, `failed_channels`
- [ ] Existing IG/FB rows still parse correctly (backward compatible)
- [ ] Column order matches `SHEET_COLUMNS` in `config_constants.py`

### Code Review
- [ ] All new files reviewed: `channels/google_business.py`, `channels/google_auth.py`, `channels/google_locations.py`, `validator.py`
- [ ] No hardcoded credentials or tokens in code
- [ ] Error messages don't leak sensitive data (tokens, secrets)
- [ ] `LOCK_TIMEOUT_MINUTES` set appropriately (default: 10)

### Testing
- [ ] All unit tests pass (`pytest tests/test_unit_core.py`)
- [ ] All E2E scenario tests pass (`pytest tests/test_e2e_scenarios.py`)
- [ ] All existing tests pass — full regression (`pytest`)
- [ ] Manual smoke test: IG-only post still works
- [ ] Manual smoke test: FB-only post still works
- [ ] Manual smoke test: IG+FB post still works

---

## Rollout (Phased)

### Phase 1: Feature Flag OFF (deploy code only)
- [ ] Deploy with `GBP_ENABLED=false`
- [ ] Verify IG/FB publishing works normally (no regression)
- [ ] Verify GBP channel is NOT registered when flag is off
- [ ] Monitor logs for any errors related to new code paths
- [ ] Wait 24h with no issues

### Phase 2: GBP Internal Testing
- [ ] Set `GBP_ENABLED=true`
- [ ] Create a test GBP-only post with `google_location_id`
- [ ] Verify GBP text-only post publishes successfully
- [ ] Verify GBP text+image post publishes successfully
- [ ] Verify GBP post appears on Google Business Profile
- [ ] Verify CTA (if used) renders correctly

### Phase 3: Mixed Channel Testing
- [ ] Create IG+GBP post → verify both publish
- [ ] Create IG+FB+GBP post → verify all three publish
- [ ] Simulate GBP failure (invalid location) → verify PARTIAL status
- [ ] Verify retry for PARTIAL GBP → only GBP retried, not IG/FB
- [ ] Verify Telegram notifications for PARTIAL/ERROR

### Phase 4: Production
- [ ] Enable for real customer posts
- [ ] Monitor first 10 GBP posts for success rate
- [ ] Verify lock recovery works for stuck PROCESSING rows
- [ ] Confirm Cloudinary cleanup handles PARTIAL rows

---

## Post-Deployment Monitoring

### First 48 Hours
- [ ] Check logs for GBP API errors (rate limits, auth failures)
- [ ] Verify no duplicate publishes (lock mechanism working)
- [ ] Monitor retry_count — rows with count > 3 need investigation
- [ ] Check Telegram alerts are firing for GBP errors

### Ongoing
- [ ] GBP OAuth token refresh working (check every 7 days)
- [ ] API quota not exceeded (check Google Cloud Console)
- [ ] PROCESSING rows not accumulating (lock timeout recovery)

---

## Rollback Plan

If issues are found:

1. **Quick rollback**: Set `GBP_ENABLED=false` → GBP channel deregistered, IG/FB unaffected
2. **Full rollback**: Revert to previous deployment — all IG/FB rows continue working
3. **Partial fix**: If only GBP has issues, rows with `network=IG+FB` are unaffected

### Rollback indicators:
- GBP API returning persistent 4xx errors
- OAuth token refresh failing repeatedly
- Lock recovery loop (rows cycling PROCESSING → READY → PROCESSING)
- IG/FB regression (any failure in existing channels = immediate rollback)
