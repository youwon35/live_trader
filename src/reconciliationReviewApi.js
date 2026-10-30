import { request } from './api.js';

// One explicit annotation request. No broker refresh, baseline mutation, retry,
// or trading authority is coupled to this UI action.
export async function submitReconciliationReview(payload) {
  return request('/api/reconciliation/review', {method:'POST',body:payload,timeoutMs:15_000});
}

