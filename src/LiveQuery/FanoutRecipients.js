// One monotonic cutoff covers all subscriptions. Iteration never includes a
// registration created after event admission, even during continuous subscribe.
let generation = 0;
export const fanoutGeneration = () => generation;

export default class FanoutRecipients {
  constructor() {
    this.created = ++generation;
    this.head = null;
    this.tail = null;
    this.clients = new Map();
    this.cursors = new Set();
    this.size = 0;
  }
  add(clientId, requestId) {
    let requests = this.clients.get(clientId);
    if (!requests) {
      requests = new Map();
      this.clients.set(clientId, requests);
    }
    if (requests.has(requestId)) { return requests.get(requestId).token; }
    // Callbacks retain only this lifetime token, never the list links. A slow
    // callback must not retain the history of subsequently removed recipients.
    const token = { clientId, requestId, generation: ++generation, active: true };
    const entry = { token, previous: this.tail, next: null };
    if (this.tail) { this.tail.next = entry; }
    else { this.head = entry; }
    this.tail = entry;
    requests.set(requestId, entry);
    this.size++;
    return token;
  }
  remove(clientId, requestId) {
    const requests = this.clients.get(clientId);
    const entry = requests?.get(requestId);
    if (!entry) { return; }
    entry.token.active = false;
    for (const cursor of this.cursors) {
      if (cursor.next === entry) { cursor.next = entry.next; }
    }
    if (entry.previous) { entry.previous.next = entry.next; }
    else { this.head = entry.next; }
    if (entry.next) { entry.next.previous = entry.previous; }
    else { this.tail = entry.previous; }
    // Suspended cursors now point beyond this entry, so no tombstone chain
    // survives even if a producer is blocked indefinitely on callback capacity.
    entry.previous = null;
    entry.next = null;
    requests.delete(requestId);
    if (!requests.size) { this.clients.delete(clientId); }
    this.size--;
  }
  *entries(cutoff) {
    const cursor = { next: this.head };
    this.cursors.add(cursor);
    try {
      while (cursor.next && cursor.next.token.generation <= cutoff) {
        const entry = cursor.next;
        // Advance before yielding. remove() can move this pointer while the
        // consumer is suspended without retaining removed nodes or skipping a
        // surviving recipient. New registrations are excluded by the cutoff.
        cursor.next = entry.next;
        yield entry.token;
      }
    } finally {
      this.cursors.delete(cursor);
    }
  }
}
