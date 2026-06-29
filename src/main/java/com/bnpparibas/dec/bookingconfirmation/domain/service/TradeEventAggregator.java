package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeAggregation;
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
 *   <li>CREATED and DELETED both present → emit nothing: the trade was born and killed before
 *       downstream ever saw it.
 *   <li>CREATED present → emit one CREATED carrying the latest payload (an amendment's payload is
 *       re-typed by the caller).
 *   <li>DELETED present without CREATED → emit the delete: downstream learned of the trade in an
 *       earlier drain and must learn of the delete. DELETED dominates regardless of its position in
 *       the group (defensive against out-of-order delivery).
 *   <li>otherwise (AMENDED only) → emit one AMENDED carrying the latest payload.
 * </ul>
 *
 * <p>A {@code null} message key cannot identify a trade, so such events pass through as singleton
 * groups. The aggregation window is one drained batch; events of one trade split across drains
 * aggregate independently, which still yields a correct downstream outcome.
 */
public class TradeEventAggregator {

    public List<TradeAggregation> aggregate(final List<ParsedTradeEvent> events) {
        final Map<Object, List<ParsedTradeEvent>> groups = new LinkedHashMap<>();
        for (final ParsedTradeEvent event : events) {
            final String messageKey = event.message().messageKey();
            // A keyless event cannot be matched to its trade: the row id makes it a singleton group.
            final Object groupKey = messageKey != null ? messageKey : event.id();
            groups.computeIfAbsent(groupKey, ignored -> new ArrayList<>()).add(event);
        }
        return groups.values().stream().map(TradeEventAggregator::collapse).toList();
    }

    private static TradeAggregation collapse(final List<ParsedTradeEvent> members) {
        boolean hasCreated = false;
        boolean hasDeleted = false;
        for (final ParsedTradeEvent member : members) {
            hasCreated |= member.type() == TradeEventType.CREATED;
            hasDeleted |= member.type() == TradeEventType.DELETED;
        }
        if (hasCreated && hasDeleted) {
            return TradeAggregation.dropAll(ids(members, null));
        }
        final ParsedTradeEvent survivor = hasDeleted ? lastOf(members, TradeEventType.DELETED) : members.getLast();
        final TradeEventType emitAs =
                hasDeleted ? TradeEventType.DELETED : hasCreated ? TradeEventType.CREATED : TradeEventType.AMENDED;
        return TradeAggregation.emit(survivor, emitAs, ids(members, survivor));
    }

    private static ParsedTradeEvent lastOf(final List<ParsedTradeEvent> members, final TradeEventType type) {
        ParsedTradeEvent last = null;
        for (final ParsedTradeEvent member : members) {
            if (member.type() == type) {
                last = member;
            }
        }
        if (last == null) {
            throw new IllegalStateException("No member of type " + type + " — caller checked presence");
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
