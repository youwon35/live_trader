"""Exact public profile history for the generic Live monitor/runtime path.

This never replaces a prepared-native handoff or authorizes an order. The
source receipt remains history-only; its bars only initialize/bridge indicators.
"""
from __future__ import annotations
from trading_runtime.stock_session_profile import profile_for_spec,profile_hash,normalize_stock_session_profile,market_for_symbol,next_profile_boundary_start
from trading_runtime.market_data import parse_timestamp,timeframe_seconds


def feed_children(feeds):
 pending=list(feeds);seen=set()
 while pending:
  item=pending.pop()
  if id(item) in seen:continue
  seen.add(id(item))
  wrapped=getattr(item,'_feed',None)
  if wrapped is not None:pending.append(wrapped);continue
  children=getattr(item,'feeds',None)
  if children is not None:pending.extend(children);continue
  yield item


def has_explicit_stock_profiles(feed):
 return any(getattr(sub,'stock_session_profile',None) is not None for child in feed_children([feed]) for sub in getattr(child,'subscriptions',()))


def exact_subscription(feeds,spec):
 from trading_runtime.realtime_feeds import FeedSubscription,_runtime_spec_kis_exchange
 if spec.provider not in {'kis','yahoo'} or spec.broker_id!='kis':
  return next(feed for feed in feeds if feed.provider_id==spec.provider),FeedSubscription(spec.instrument_id,spec.symbol,spec.timeframe)
 expected_profile=profile_for_spec(spec);expected_exchange=_runtime_spec_kis_exchange(spec);matches=[]
 for child in feed_children(feeds):
  if child.provider_id!=spec.provider:continue
  for sub in getattr(child,'subscriptions',()):
   if (sub.instrument_id,sub.symbol,sub.timeframe,sub.exchange)!=(spec.instrument_id,spec.symbol,spec.timeframe,expected_exchange):continue
   actual=normalize_stock_session_profile(getattr(sub,'stock_session_profile',None),market=market_for_symbol(sub.symbol),provider='kis')
   if actual==expected_profile:matches.append((child,sub))
 if len(matches)!=1:raise ValueError('LIVE_KIS_EXACT_PROFILE_EXCHANGE_SUBSCRIPTION_REQUIRED')
 return matches[0]


def seed_profile_history(feeds,spec,engine,limit):
 owner,sub=exact_subscription(feeds,spec)
 if getattr(sub,'stock_session_profile',None) is None:
  return engine.seed_history(spec.strategy_instance_id,owner.warmup(sub,limit))
 from trading_runtime.provider_backfill_kis_sessions import verify_segment
 bars,receipt=owner.authoritative_warmup(sub,limit=limit)
 if [bar.to_dict() for bar in verify_segment(receipt)]!=[bar.to_dict() for bar in bars]:raise ValueError('LIVE_KIS_PROFILE_HISTORY_REPRODUCTION_CHANGED')
 engine.set_session_windows(spec,receipt['windows'])
 count=engine.seed_history(spec.strategy_instance_id,bars)
 owner.mark_emitted(bars)
 # Store source windows/receipt outside the strategy artifact, which is sealed.
 if not hasattr(owner,'_profile_history_receipts'):owner._profile_history_receipts={}
 owner._profile_history_receipts[profile_hash(profile_for_spec(spec))]=receipt
 return count


def configure_profile_bridges(feeds,specs,engine):
 from trading_runtime.provider_backfill import verify_segment
 if getattr(engine,'native_initialization',None) is not None:raise ValueError('LIVE_PREPARED_NATIVE_CANNOT_USE_GENERIC_PROFILE_HISTORY')
 for child in feed_children(feeds):
  if not any(getattr(sub,'stock_session_profile',None) is not None for sub in getattr(child,'subscriptions',())):continue
  scoped=[]
  for spec in specs:
   if spec.provider!='kis':continue
   try:owner,_subscription=exact_subscription([child],spec)
   except ValueError:continue
   if owner is child:scoped.append(spec)
  def accept(members,bars,receipt):
   verified=verify_segment(receipt)
   if [bar.to_dict() for bar in verified]!=[bar.to_dict() for bar in bars]:raise ValueError('LIVE_PROFILE_PARTIAL_RECEIPT_CHANGED')
   if receipt.get('historicalOnly') is not True or receipt.get('naturalForwardCycles')!=0 or receipt.get('brokerOrders')!=0:raise ValueError('LIVE_PROFILE_PARTIAL_HISTORY_AUTHORITY_INVALID')
   for spec in members:
    history=engine.histories[spec.strategy_instance_id]
    if not history or not bars:raise ValueError('LIVE_PROFILE_PARTIAL_HISTORY_ANCHOR_REQUIRED')
    selected=profile_for_spec(spec)
    expected=next_profile_boundary_start(selected,history[-1].end_time,timeframe_seconds(spec.timeframe),windows=receipt.get('windows'))
    if parse_timestamp(bars[0].start_time)!=expected:raise ValueError('LIVE_PROFILE_PARTIAL_HISTORY_DOES_NOT_JOIN')
    if receipt.get('windows'):engine.set_session_windows(spec,receipt['windows'])
    engine.seed_history(spec.strategy_instance_id,bars)
  if scoped:child.configure_warmup_bridge(scoped,accept,kis_rest_fetcher=getattr(child,'_startup_kis_rest_fetcher',None))
