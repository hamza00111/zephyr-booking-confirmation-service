package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Per-trade create-barrier state (one per {@code (region, message key)}). The downstream third party
 * rejects an AMEND/BUST whose CREATE it never received, so an AMEND must never be relayed before its
 * trade's CREATE has reached {@code SENT}.
 *
 * <pre>
 * NONE ──emit CREATE──► IN_FLIGHT ──CREATE delivered──► SENT
 *   │                       │
 *   │                       └──CREATE failed terminally──► FAILED
 *   └──busted before any CREATE emitted──► VOID
 * </pre>
 *
 * <p>{@code NONE} is the implicit state when no gate row exists yet.
 */
public enum CreateState {
    /** No CREATE seen yet for this trade. */
    NONE,
    /** A CREATE has been staged to the outbox but is not yet delivered — amendments wait (BLOCKED). */
    IN_FLIGHT,
    /** A CREATE has been delivered downstream — amendments may flow. */
    SENT,
    /** The CREATE failed terminally (INVALID/PARKED); a waiting AMEND is auto-promoted into a CREATE. */
    FAILED,
    /** The trade was busted before any CREATE was emitted — nothing is published for it. */
    VOID
}
