export function runtimeBatchFromResponse(result, expectedBatchId) {
  const batch = result?.batch;
  if (typeof result?.ok !== 'boolean' || !batch || typeof batch.batchId !== 'string'
      || !batch.batchId || typeof batch.scopeHash !== 'string' || !batch.scopeHash
      || !Array.isArray(batch.members) || !batch.members.length
      || batch.members.some(member => !member || typeof member.memberId !== 'string'
        || !member.memberId || typeof member.status !== 'string')
      || (expectedBatchId && batch.batchId !== expectedBatchId)) {
    throw Error('같은 구성의 상태를 확인하지 못했습니다. 실행 결과를 다시 조회하세요.');
  }
  return batch;
}

export function runtimeBatchReview(batch) {
  const members = batch?.members || [];
  const ready = members.filter(member => member.status === 'PREPARED' && !member.attempted).length;
  const blocked = members.filter(member => member.status === 'BLOCKED').length;
  const running = members.filter(member => member.status === 'RUNNING').length;
  let message;
  if (running) message = `현재 ${running}개 경로가 실행 중입니다. 아래에서 각 경로의 상태를 확인하세요.`;
  else if (blocked === members.length && blocked) message = '모든 경로가 차단되어 시작할 수 없습니다. 아래 사유를 해결한 뒤 구성을 다시 확인하세요.';
  else if (blocked) message = `시작 전 구성 ${ready}개 · 차단 ${blocked}개입니다. 차단 사유와 실행 순서를 확인하세요.`;
  else if (ready === members.length && ready) message = `선택한 ${ready}개 구성의 연결 정보를 확인했습니다. 계좌·시장·운용 한도는 시작할 때 다시 검사합니다.`;
  else message = '아래 실행 결과를 확인하세요. 결과가 미확인이면 새 실행을 시작하지 않습니다.';
  return { ready, blocked, running, message, canStart: ready > 0 };
}

export function runtimeBatchReason(code) {
  return ({
    BATCH_MEMBER_SELECTION_BLOCKED: '전략과 계좌의 연결 범위를 확인하지 못했습니다. 운용 설정을 확인한 뒤 구성을 다시 확인하세요.',
    BATCH_MEMBER_BINDING_CHANGED: '전략 또는 계좌 연결이 변경되었습니다. 구성을 다시 확인하세요.',
    BATCH_DEPLOYMENT_ALREADY_ACTIVE: '같은 전략이 이미 실행 중이거나 종료를 확인하고 있습니다. 실시간 감시에서 확인하세요.',
    BATCH_DEPLOYMENT_START_PENDING_OR_UNKNOWN: '같은 전략의 시작 결과가 미확인입니다. 기존 실행 결과를 먼저 확인하세요.',
    BATCH_RUNTIME_OBSERVATION_UNAVAILABLE: '현재 실행 상태를 읽지 못했습니다. 연결 상태를 확인한 뒤 결과를 다시 조회하세요.',
    BATCH_OWNER_RESTARTED_NO_RESUME: '프로그램을 다시 열어 이전 시작 결과가 미확인입니다. 자동으로 재시작하지 않습니다.',
    BATCH_RESULT_PERSISTENCE_UNKNOWN: '시작 결과가 저장되었는지 확인하지 못했습니다. 새 실행 없이 기존 결과를 확인하세요.',
    BATCH_START_NOT_OBSERVED_RUNNING: '시작 요청 후 실제 실행 중인 상태를 아직 확인하지 못했습니다.',
  })[code] || code || '';
}
