# SafiChat Operator Guide — Local Business Orchestration (One-Page Guide)

**Audience:** Small-business owner or independent SAFI tester (Acceptance Test A12).
**Rule:** Every step below is performed entirely inside native SafiChat. You never need to open n8n, Calnode, or Windmill admin dashboards.

---

## 1. Running a Customer Process (Ungated Mode)
By default, active customer conversations run in **Ungated** mode within your approved permissions and allowlisted test contacts.

1. **Marketing Intake (n8n):** Start lead intake for a customer conversation (e.g. Subject Line: `Customer A - Inquiry`). SafiChat ingests the inquiry and prepares the approved follow-up through the Gate Adapter.
2. **Sales Availability & Booking (Calnode):** Request available appointment slots for the customer, pick an available slot, and request a booking. SafiChat returns the confirmed `Booking ID`. Submitting the same booking twice will not double-book.
3. **Operations Assignment (Windmill):** Trigger the operational assignment for that `Booking ID`. SafiChat displays the Windmill `Job ID`, state (`COMPLETED` / `FAILED`), assignee, and completion result.

---

## 2. Taking Control: Barge In & Gated Mode
At any time, you can intervene on **one specific conversation** without disrupting other active customer conversations:

1. **Select by Subject Line & Barge In:**
   - Request **Barge In** using the conversation's Subject Line (e.g. `Customer B - Quote`).
   - If two conversations share the same Subject Line, SafiChat will display a candidate list of `ConversationID`s and ask you to pick the exact one.
   - Wait for the confirmed **`GATED`** status. Any action already dispatched before confirmation is reconciled and reported truthfully (never falsely promised undone). No new controlled action starts once confirmed.
2. **Review Held Actions:**
   - New outbound follow-ups, booking requests, or operational assignments in that conversation are automatically **HELD**.
   - Ask SafiChat to list held actions for the conversation.
3. **Approve or Reject Individual Actions:**
   - **Approve** a held action to recheck your current permissions and execute it once.
   - **Reject** a held action to cancel it permanently without external effect.

---

## 3. Returning to Ungated Mode
1. Request **Return to Ungated** for the gated conversation.
2. SafiChat presents every remaining held action. Choose **Release** or **Cancel** for each item.
3. Only released, still-authorized actions execute (at most once per idempotency key). Cancelled actions never run.
4. SafiChat confirms **`UNGATED`** once all held items are resolved.

---

## 4. Safety & Fail-Closed Recovery
- If permissions are revoked, a recipient is not allowlisted, or a service outcome is uncertain (timeout/restart mid-dispatch), the conversation safely enters **`FAILED_CLOSED`** and dispatches nothing until reconciled by an authorized operator.
