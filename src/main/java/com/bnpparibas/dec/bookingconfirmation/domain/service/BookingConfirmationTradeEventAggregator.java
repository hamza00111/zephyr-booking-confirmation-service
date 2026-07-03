package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.BookingConfirmationAggregation;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Pure, framework-free implementation of the per-trade aggregation rules applied between inbox and
 * outbox (PROCESS stage).
 *
 * <p>Events are grouped by Kafka message key — the trade reference, identical across all events of
 * one trade — in arrival order (inbox id order; same key ⇒ same partition ⇒ offsets are ingested
 * in order). Per group:
 *
 * <ul>
 *   <li>TRADE_CREATED and TRADE_DELETED both present → emit nothing: the trade was born and killed before
 *       downstream ever saw it.
 *   <li>TRADE_CREATED present → emit one TRADE_CREATED carrying the latest payload (an amendment's payload is
 *       re-typed by the caller).
 *   <li>TRADE_DELETED present without TRADE_CREATED → emit the delete: downstream learned of the trade in an
 *       earlier drain and must learn of the delete. TRADE_DELETED dominates regardless of its position in
 *       the group (defensive against out-of-order delivery).
 *   <li>otherwise (TRADE_AMENDED only) → emit one TRADE_AMENDED carrying the latest payload.
 * </ul>
 *
 * <p>A {@code null} message key cannot identify a trade, so such events pass through as singleton
 * groups. The aggregation window is one drained batch; events of one trade split across drains
 * aggregate independently, which still yields a correct downstream outcome.
 */
public class BookingConfirmationTradeEventAggregator {

    public List<BookingConfirmationAggregation> aggregate(final List<ParsedTradeEvent> events) {
        final Map<Object, List<ParsedTradeEvent>> groups = new LinkedHashMap<>();
        for (final ParsedTradeEvent event : events) {
            final String messageKey = event.message().messageKey();
            // A keyless event cannot be matched to its trade: its own identity makes it a singleton
            // group (the row id is nullable, and null ids must not collapse keyless events together).
            final Object groupKey = messageKey != null ? messageKey : event;
            groups.computeIfAbsent(groupKey, ignored -> new ArrayList<>()).add(event);
        }
        return groups.values().stream().map(BookingConfirmationTradeEventAggregator::collapse).toList();
    }

    private static BookingConfirmationAggregation collapse(final List<ParsedTradeEvent> members) {
        boolean hasCreated = false;
        boolean hasDeleted = false;
        for (final ParsedTradeEvent member : members) {
            hasCreated |= member.eventType() == TradeEventType.TRADE_CREATED;
            hasDeleted |= member.eventType() == TradeEventType.TRADE_DELETED;
        }
        if (hasCreated && hasDeleted) {
            return BookingConfirmationAggregation.dropAll(ids(members, null));
        }
        final ParsedTradeEvent survivor = hasDeleted ? lastDeletedOf(members) : members.getLast();
        final TradeEventType createdOrAmended =
                hasCreated ? TradeEventType.TRADE_CREATED : TradeEventType.TRADE_AMENDED;
        final TradeEventType emitAs = hasDeleted ? TradeEventType.TRADE_DELETED : createdOrAmended;
        return BookingConfirmationAggregation.emit(survivor, emitAs, ids(members, survivor));
    }

    private static ParsedTradeEvent lastDeletedOf(final List<ParsedTradeEvent> members) {
        ParsedTradeEvent last = null;
        for (final ParsedTradeEvent member : members) {
            if (member.eventType() == TradeEventType.TRADE_DELETED) {
                last = member;
            }
        }
        if (last == null) {
            throw new IllegalStateException("No TRADE_DELETED member — caller checked presence");
        }
        return last;
    }

    private static List<Long> ids(final List<ParsedTradeEvent> members, @Nullable final ParsedTradeEvent excluded) {
        final List<Long> ids = new ArrayList<>(members.size());
        for (final ParsedTradeEvent member : members) {
            if (member != excluded) {
                ids.add(member.id());
            }
        }
        return ids;
    }
}
