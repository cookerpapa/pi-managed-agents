// A ticket identifies the observation a possibly concurrent PG read can clear.
// Start conservatively until the lease's bootstrap snapshot has been checked.
export class SteerHint {
  #ticket = Symbol();
  get ticket() {
    return this.#ticket;
  }
  mark() {
    this.#ticket = Symbol();
  }
  clear(ticket) {
    if (this.#ticket === ticket) this.#ticket = null;
  }
}
