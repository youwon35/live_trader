import test from 'node:test';
import assert from 'node:assert/strict';
import {coordinationRetirementView as view} from '../src/coordinationRetirement.js';
test('only explicit complete releases appear as released',()=>{
 assert.equal(view({api_connected:true,order_coordination_retirement:{a:{releasedScopes:2}}}),null);
 assert.equal(view({api_connected:true,order_coordination_retirement:{a:{retained:false,reconciliationRequired:false,releasedScopes:2}}}).pending,false);
});
test('unresolved owner dominates partial released scopes',()=>{
 const result=view({api_connected:true,order_coordination_retirement:{a:{retained:true,releasedScopes:1,blockers:[{symbol:'SOLUSDT',reason:'COORDINATION_FUTURES_POSITION_NOT_FLAT'}]}}});
 assert.equal(result.pending,true);assert.match(result.details[0],/보유 수량/);
});
test('lost API cannot reuse positive release status as current',()=>{
 const result=view({api_connected:false,order_coordination_retirement:{a:{retained:false,reconciliationRequired:false,releasedScopes:2}}});
 assert.equal(result.pending,true);assert.match(result.label,/현재 상태 확인 필요/);
});
