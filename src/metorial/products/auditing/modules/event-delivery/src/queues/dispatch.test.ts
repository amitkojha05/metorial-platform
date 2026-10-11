import { beforeEach, describe, expect, it, vi } from 'vitest';

let attemptDeliveryAddManyWithOps = vi.fn();
let eventDeliveryIntentCreateMany = vi.fn();
let eventDeliveryIntentFindMany = vi.fn();
let eventDestinationListenerFindMany = vi.fn();
let systemEventFindUnique = vi.fn();
let dispatchProcessor: (data: { systemEventId: string; cursor?: string }) => Promise<void>;

vi.mock('./attempt', () => ({
  attemptDeliveryQueue: { addManyWithOps: attemptDeliveryAddManyWithOps }
}));
vi.mock('@metorial/db', () => ({
  db: {
    eventDeliveryIntent: {
      createMany: eventDeliveryIntentCreateMany,
      findMany: eventDeliveryIntentFindMany
    },
    eventDestinationListener: { findMany: eventDestinationListenerFindMany },
    systemEvent: { findUnique: systemEventFindUnique }
  },
  ID: { generateId: vi.fn(async () => 'evtdi_1') },
  withTransaction: vi.fn()
}));
vi.mock('@metorial/queue', () => ({
  createQueue: vi.fn(() => ({
    add: vi.fn(),
    addMany: vi.fn(),
    addManyWithOps: vi.fn(),
    process: vi.fn(callback => {
      dispatchProcessor = callback;
      return {};
    })
  })),
  QueueRetryError: class extends Error {}
}));

let { matchesListener } = await import('./dispatch');

let event = (overrides: Record<string, any> = {}) =>
  ({
    source: 'resource',
    eventType: 'organization.created',
    callbackId: null,
    callbackTriggerKey: null,
    chatConnectionId: null,
    providerId: null,
    ...overrides
  }) as any;

let listener = (overrides: Record<string, any> = {}) =>
  ({
    type: 'event',
    eventTypes: [],
    triggers: [],
    callbackId: null,
    chatConnectionId: null,
    providerId: null,
    eventDestination: { status: 'active' },
    ...overrides
  }) as any;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('event delivery dispatch', () => {
  it('creates and schedules one delivery when multiple listeners match the same destination', async () => {
    systemEventFindUnique.mockResolvedValue(
      event({ id: 'evt_1', oid: BigInt(10), organizationOid: BigInt(20), instanceOid: null })
    );
    eventDestinationListenerFindMany.mockResolvedValue([
      listener({
        oid: BigInt(30),
        eventDestinationOid: BigInt(40),
        eventTypes: ['organization.created'],
        eventDestination: {
          status: 'active',
          type: 'webhook',
          retryStrategy: 'exponential',
          retryMaxAttempts: 5,
          retryBaseDelaySeconds: 10,
          retryMaxDelaySeconds: 300
        }
      }),
      listener({
        oid: BigInt(31),
        eventDestinationOid: BigInt(40),
        eventTypes: ['organization.created'],
        eventDestination: {
          status: 'active',
          type: 'webhook',
          retryStrategy: 'exponential',
          retryMaxAttempts: 5,
          retryBaseDelaySeconds: 10,
          retryMaxDelaySeconds: 300
        }
      })
    ]);
    eventDeliveryIntentFindMany.mockResolvedValue([{ id: 'evtdi_1' }]);

    await dispatchProcessor({ systemEventId: 'evt_1' });

    expect(eventDeliveryIntentCreateMany).toHaveBeenCalledWith(
      expect.objectContaining({
        data: [
          expect.objectContaining({
            eventDestinationOid: BigInt(40),
            eventDestinationListenerOid: BigInt(30),
            systemEventOid: BigInt(10)
          })
        ],
        skipDuplicates: true
      })
    );
    expect(eventDeliveryIntentFindMany).toHaveBeenCalledWith({
      where: {
        systemEventOid: BigInt(10),
        eventDestinationOid: { in: [BigInt(40)] },
        status: 'pending',
        attemptCount: 0
      },
      select: { id: true }
    });
    expect(attemptDeliveryAddManyWithOps).toHaveBeenCalledWith([
      {
        data: { intentId: 'evtdi_1', attemptNumber: 1 },
        opts: { id: 'event-delivery-attempt:evtdi_1:1' }
      }
    ]);
  });

  it('does not schedule a delivery that has already been attempted', async () => {
    systemEventFindUnique.mockResolvedValue(
      event({ id: 'evt_1', oid: BigInt(10), organizationOid: BigInt(20), instanceOid: null })
    );
    eventDestinationListenerFindMany.mockResolvedValue([
      listener({
        oid: BigInt(30),
        eventDestinationOid: BigInt(40),
        eventTypes: ['organization.created'],
        eventDestination: {
          status: 'active',
          type: 'webhook',
          retryStrategy: 'exponential',
          retryMaxAttempts: 5,
          retryBaseDelaySeconds: 10,
          retryMaxDelaySeconds: 300
        }
      })
    ]);
    eventDeliveryIntentFindMany.mockResolvedValue([]);

    await dispatchProcessor({ systemEventId: 'evt_1' });

    expect(eventDeliveryIntentCreateMany).toHaveBeenCalledWith(
      expect.objectContaining({ skipDuplicates: true })
    );
    expect(attemptDeliveryAddManyWithOps).not.toHaveBeenCalled();
  });

  it('schedules the deliveries again when a retried dispatch finds them already created', async () => {
    systemEventFindUnique.mockResolvedValue(
      event({ id: 'evt_1', oid: BigInt(10), organizationOid: BigInt(20), instanceOid: null })
    );
    eventDestinationListenerFindMany.mockResolvedValue([
      listener({
        oid: BigInt(30),
        eventDestinationOid: BigInt(40),
        eventTypes: ['organization.created'],
        eventDestination: {
          status: 'active',
          type: 'webhook',
          retryStrategy: 'exponential',
          retryMaxAttempts: 5,
          retryBaseDelaySeconds: 10,
          retryMaxDelaySeconds: 300
        }
      })
    ]);

    // First run: the delivery is created, but enqueueing its first attempt fails.
    eventDeliveryIntentFindMany.mockResolvedValue([{ id: 'evtdi_1' }]);
    attemptDeliveryAddManyWithOps.mockRejectedValueOnce(new Error('redis unavailable'));

    await expect(dispatchProcessor({ systemEventId: 'evt_1' })).rejects.toThrow(
      'redis unavailable'
    );

    // The queue retries the job. The delivery already exists, so nothing new is created.
    await dispatchProcessor({ systemEventId: 'evt_1' });

    expect(attemptDeliveryAddManyWithOps).toHaveBeenCalledTimes(2);
    expect(attemptDeliveryAddManyWithOps).toHaveBeenLastCalledWith([
      {
        data: { intentId: 'evtdi_1', attemptNumber: 1 },
        opts: { id: 'event-delivery-attempt:evtdi_1:1' }
      }
    ]);
  });
});

describe('matchesListener', () => {
  it('skips an archived destination', () => {
    expect(
      matchesListener(
        event(),
        listener({
          eventTypes: ['organization.created'],
          eventDestination: { status: 'archived' }
        })
      )
    ).toBe(false);
  });

  describe('resource events', () => {
    it('matches a listener subscribed to the event type', () => {
      expect(
        matchesListener(event(), listener({ eventTypes: ['organization.created'] }))
      ).toBe(true);
    });

    it('does not match a listener subscribed to other event types', () => {
      expect(matchesListener(event(), listener({ eventTypes: ['session.created'] }))).toBe(
        false
      );
    });

    it('does not match a listener with no event types', () => {
      expect(matchesListener(event(), listener())).toBe(false);
    });

    it('does not match a callback or chat listener', () => {
      expect(
        matchesListener(
          event(),
          listener({ type: 'callback', callbackId: 'clb_1', triggers: [] })
        )
      ).toBe(false);
      expect(
        matchesListener(
          event(),
          listener({ type: 'chat', eventTypes: ['organization.created'] })
        )
      ).toBe(false);
    });
  });

  describe('callback events', () => {
    let callbackEvent = event({
      source: 'callback',
      eventType: 'callback.issue.created',
      callbackId: 'clb_1',
      callbackTriggerKey: 'issue.created'
    });

    it('matches every trigger when no trigger filter is set', () => {
      expect(
        matchesListener(
          callbackEvent,
          listener({ type: 'callback', callbackId: 'clb_1', triggers: [] })
        )
      ).toBe(true);
    });

    it('matches a listed trigger', () => {
      expect(
        matchesListener(
          callbackEvent,
          listener({ type: 'callback', callbackId: 'clb_1', triggers: ['issue.created'] })
        )
      ).toBe(true);
    });

    it('does not match an unlisted trigger', () => {
      expect(
        matchesListener(
          callbackEvent,
          listener({ type: 'callback', callbackId: 'clb_1', triggers: ['issue.closed'] })
        )
      ).toBe(false);
    });

    it('does not match another callback', () => {
      expect(
        matchesListener(
          callbackEvent,
          listener({ type: 'callback', callbackId: 'clb_2', triggers: [] })
        )
      ).toBe(false);
    });

    it('matches a provider-tier listener when the event carries that provider', () => {
      expect(
        matchesListener(
          event({
            source: 'callback',
            eventType: 'callback.issue.created',
            callbackId: 'clb_1',
            callbackTriggerKey: 'issue.created',
            providerId: 'prov_1'
          }),
          listener({ type: 'callback', providerId: 'prov_1', triggers: [] })
        )
      ).toBe(true);
    });

    it('does not match a provider-tier listener for another provider', () => {
      expect(
        matchesListener(
          event({
            source: 'callback',
            eventType: 'callback.issue.created',
            callbackId: 'clb_1',
            callbackTriggerKey: 'issue.created',
            providerId: 'prov_1'
          }),
          listener({ type: 'callback', providerId: 'prov_2', triggers: [] })
        )
      ).toBe(false);
    });

    it('never matches a provider-tier listener when the event has no resolvable provider', () => {
      expect(
        matchesListener(
          callbackEvent,
          listener({ type: 'callback', providerId: 'prov_1', triggers: [] })
        )
      ).toBe(false);
    });

    it('matches a pure ALL-tier listener regardless of callback or provider', () => {
      expect(
        matchesListener(callbackEvent, listener({ type: 'callback', triggers: [] }))
      ).toBe(true);
    });
  });

  describe('chat events', () => {
    let chatEvent = event({
      source: 'chat',
      eventType: 'chat.message.received',
      chatConnectionId: 'chi_1'
    });

    it('matches the right connection and event type', () => {
      expect(
        matchesListener(
          chatEvent,
          listener({
            type: 'chat',
            chatConnectionId: 'chi_1',
            eventTypes: ['chat.message.received']
          })
        )
      ).toBe(true);
    });

    it('does not match another connection', () => {
      expect(
        matchesListener(
          chatEvent,
          listener({
            type: 'chat',
            chatConnectionId: 'chi_2',
            eventTypes: ['chat.message.received']
          })
        )
      ).toBe(false);
    });

    it('does not match an unsubscribed event type', () => {
      expect(
        matchesListener(
          chatEvent,
          listener({
            type: 'chat',
            chatConnectionId: 'chi_1',
            eventTypes: ['chat.member.joined']
          })
        )
      ).toBe(false);
    });

    it('does not match a system event listener', () => {
      expect(
        matchesListener(
          chatEvent,
          listener({ type: 'event', eventTypes: ['chat.message.received'] })
        )
      ).toBe(false);
    });

    it('matches a provider-tier listener when the event carries that provider', () => {
      expect(
        matchesListener(
          event({
            source: 'chat',
            eventType: 'chat.message.received',
            chatConnectionId: 'chi_1',
            providerId: 'prov_1'
          }),
          listener({
            type: 'chat',
            providerId: 'prov_1',
            eventTypes: ['chat.message.received']
          })
        )
      ).toBe(true);
    });

    it('does not match a provider-tier listener for another provider', () => {
      expect(
        matchesListener(
          event({
            source: 'chat',
            eventType: 'chat.message.received',
            chatConnectionId: 'chi_1',
            providerId: 'prov_1'
          }),
          listener({
            type: 'chat',
            providerId: 'prov_2',
            eventTypes: ['chat.message.received']
          })
        )
      ).toBe(false);
    });

    it('never matches a provider-tier listener when the event has no resolvable provider', () => {
      expect(
        matchesListener(
          chatEvent,
          listener({
            type: 'chat',
            providerId: 'prov_1',
            eventTypes: ['chat.message.received']
          })
        )
      ).toBe(false);
    });

    it('matches a pure ALL-tier listener regardless of connection or provider', () => {
      expect(
        matchesListener(
          chatEvent,
          listener({ type: 'chat', eventTypes: ['chat.message.received'] })
        )
      ).toBe(true);
    });
  });
});
