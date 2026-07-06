#!/usr/bin/env python3
"""High-throughput trade-event producer for aggregation testing.

Sends a continuous stream of TradeCreated/TradeAmended (optionally TradeDeleted)
events to the internal topic, shaped like the real upstream publisher's messages:
message key ``<firm>_<pivotId.id>`` (e.g. ``KOP_444071454_1``), an
``idempotency-key`` header unique per event, and the full UBIX payload envelope.

Aggregation math (printed after the run): the PROCESS stage groups events per
message key *within one drained batch* (default batch-size 200, tick every 3s).
Per (trade, drain) group: Created+Deleted -> nothing; Created present -> one
TradeCreated with the latest payload; Deleted alone -> one TradeDeleted;
otherwise one TradeAmended. So the expected published count is one event per
trade, plus at most a couple extra when a drain boundary splits a trade's run.

Usage:
    pip install kafka-python
    python3 scripts/produce_trade_events.py                      # 100 trades x 5 events
    python3 scripts/produce_trade_events.py --trades 1           # 500 events, one key
    python3 scripts/produce_trade_events.py --delete-last        # every trade ends deleted
    python3 scripts/produce_trade_events.py --rate 50            # throttle to 50 events/s
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

try:
    from kafka import KafkaProducer
except ImportError:
    sys.exit("kafka-python is required: pip install kafka-python")


def parse_args():
    parser = argparse.ArgumentParser(description="Produce trade events for aggregation testing")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="local.booking.trade.internal.amer")
    parser.add_argument("--trades", type=int, default=100, help="distinct trades (message keys)")
    parser.add_argument("--events-per-trade", type=int, default=5,
                        help="events per trade: 1 TradeCreated + N-1 TradeAmended")
    parser.add_argument("--delete-last", action="store_true",
                        help="append a trailing TradeDeleted to every trade")
    parser.add_argument("--rate", type=float, default=0,
                        help="events per second; 0 = as fast as possible")
    parser.add_argument("--base-trade-id", type=int, default=444071454)
    parser.add_argument("--firm", default="KOP")
    parser.add_argument("--hub", default="US")
    parser.add_argument("--source-id", default="KOP")
    return parser.parse_args()


def iso_instant(moment):
    """ISO-8601 with nanosecond-style precision, as the real producer emits."""
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond:06d}000Z"


def build_event(args, event_type, trade_id, amendment_index, now):
    """One full envelope; amendments bump tradeUpdateDateTime and notional."""
    update_time = now + timedelta(seconds=amendment_index)
    return {
        "version": "1.0",
        "eventType": event_type,
        "eventId": str(uuid.uuid4()),
        "pivotId": {"id": trade_id},
        "traceId": str(uuid.uuid4()),
        "externalSystem": "UBIX",
        "flowDirection": "INBOUND",
        "hub": args.hub,
        "occurredAt": iso_instant(now),
        "recordedAt": iso_instant(now),
        "auditInfo": {
            "initiator": "Booking confirmation publisher",
            "reason": "Automated booking confirmation publishing workflow",
            "comment": "Zephyr booking confirmation publisher service stream",
        },
        "payload": {
            "externalSystemTradeId": {"id": trade_id},
            "externalSystem": {"name": "UBIX", "description": "Ubix back-office"},
            "references": {
                "externalSystemTradeId": trade_id,
                "originalTradeId": None,
                "secondaryTradeId": None,
                "previousTradeId": None,
                "universalTradeId": None,
                "basketId": None,
                "orderId": "G182559711",
                "reportTrackingNumber": None,
            },
            "tradeUpdateDateTime": update_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "matchingStatus": "MATCHED",
            "clearingStatus": "CLEARED",
            # Bumped per amendment so "latest payload wins" is visible downstream.
            "notional": 1000000 + amendment_index,
            "positionId": None,
            "direction": "SELL",
            "quantity": {"value": 1},
            "price": {"value": 100000},
            "parties": {
                "clearingHouse": None,
                "exchange": {"name": "XSIM", "lei": None},
                "operatingFirm": {"name": "SG S007", "lei": None},
                "counterParty": {"name": "SGP C", "lei": None},
                "executingFirm": {"name": "MS", "lei": None},
                "giveUpFirm": None,
                "sendingFirm": {"name": "MS", "lei": None},
                "executingBroker": None,
                "trader": None,
                "accountOwner": {"name": "D31500", "lei": None},
            },
            "counterpartyAccount": {"code": "SGP C"},
            "lifeCycle": {
                "tradeDate": now.strftime("%Y-%m-%d"),
                "executionTime": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "clearingDate": now.strftime("%Y-%m-%d"),
                "marketDateTime": None,
            },
            "tradeSubType": {"code": None, "name": None},
            "fees": [
                {"type": "EXCHANGE_FEE", "amount": 0, "tax": 0, "currency": None},
                {"type": "EXECUTION_FEE", "amount": 0, "tax": 0, "currency": None},
                {"type": "CLEARING_FEE", "amount": 0, "tax": 0, "currency": None},
                {"type": "NFA_FEE", "amount": 0, "tax": 0, "currency": None},
            ],
            "sessionId": None,
            "lastCapacity": None,
            "tradingVenue": None,
            "venueType": {"code": "N", "name": None},
            "executionSource": {"code": None, "name": None},
            "priceType": None,
            "allocationAccount": None,
            "positionAccount": None,
        },
    }


def headers_for(args, trade_id, sequence):
    idempotency_key = f"{args.hub}:{args.source_id}:{args.firm}:{trade_id}:{sequence}"
    return [
        ("idempotency-key", idempotency_key.encode()),
        ("source-id", args.source_id.encode()),
        ("firm", args.firm.encode()),
        ("hub", args.hub.encode()),
        ("booking-confirmation-id", str(sequence).encode()),
    ]


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=str.encode,
        value_serializer=lambda value: json.dumps(value).encode(),
        acks="all",
        linger_ms=5,
    )

    per_trade = ["TradeCreated"] + ["TradeAmended"] * (args.events_per_trade - 1)
    if args.delete_last:
        per_trade.append("TradeDeleted")

    sent = {"TradeCreated": 0, "TradeAmended": 0, "TradeDeleted": 0}
    sequence = 0
    delay = 1.0 / args.rate if args.rate > 0 else 0
    started = time.time()

    for i in range(args.trades):
        trade_id = f"{args.base_trade_id + i}_1"
        message_key = f"{args.firm}_{trade_id}"
        now = datetime.now(timezone.utc)
        for amendment_index, event_type in enumerate(per_trade):
            sequence += 1
            event = build_event(args, event_type, trade_id, amendment_index, now)
            producer.send(
                args.topic,
                key=message_key,
                value=event,
                headers=headers_for(args, trade_id, sequence),
            )
            sent[event_type] += 1
            if sequence % 100 == 0:
                print(f"  ... {sequence} events sent")
            if delay:
                time.sleep(delay)

    producer.flush()
    producer.close()
    elapsed = time.time() - started
    total = sum(sent.values())

    print()
    print(f"Sent {total} events in {elapsed:.1f}s ({total / max(elapsed, 0.001):.0f}/s) "
          f"to {args.topic}")
    print(f"  {sent['TradeCreated']} TradeCreated, {sent['TradeAmended']} TradeAmended, "
          f"{sent['TradeDeleted']} TradeDeleted across {args.trades} trade key(s) "
          f"({args.firm}_{args.base_trade_id}_1 ...)")
    print()
    if args.delete_last:
        print("Expected after aggregation: 0 published events — every trade's group contains")
        print(f"Created+Deleted, which nets out. Inbox: all {total} rows AGGREGATED.")
    elif args.trades == 1:
        print("Expected after aggregation: ONE published event per PROCESS drain that saw the key")
        print(f"(batch-size 200, tick 3s) — roughly ceil({total}/200) at full speed, more if")
        print("produced slower than the tick. The first is TradeCreated, the rest TradeAmended.")
    else:
        print(f"Expected after aggregation: {args.trades} published events (one TradeCreated per")
        print(f"trade, carrying the LAST amendment's payload — notional {1000000 + len(per_trade) - 1}).")
        print(f"Inbox: {args.trades} PROCESSED + {total - args.trades} AGGREGATED. A drain boundary")
        print("splitting a trade's run adds one extra event for that trade (+1/+2 typical worst case).")
    print()
    print("Verify with:")
    print("  SELECT PROCESSING_STATUS, COUNT(*) FROM BOOKING_CONFIRMATION_INBOX GROUP BY PROCESSING_STATUS;")
    print("  SELECT PROCESSING_STATUS, COUNT(*) FROM BOOKING_CONFIRMATION_OUTBOX GROUP BY PROCESSING_STATUS;")


if __name__ == "__main__":
    main()
