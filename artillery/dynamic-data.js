const { v4: uuidv4 } = require("uuid");

module.exports = {
  generateCheckpoint: function (userContext, events, done) {
    const id = uuidv4();
    const timestamp = new Date().toISOString();

    userContext.vars.checkpoint_id = id;
    userContext.vars.checkpoint = {
      checkpoint_id: id,
      config: {
        configurable: {
          thread_id: uuidv4(),
          checkpoint_ns: "default",
          checkpoint_id: id
        }
      },
      checkpoint: {
        v: 1,
        ts: timestamp,
        id: id,
        channel_values: {
          my_key: `value-${Math.floor(Math.random() * 1000)}`,
          node: `node-${Math.floor(Math.random() * 100)}`
        },
        channel_versions: {
          __start__: 1,
          my_key: 2,
          "start:node": 3,
          node: 4
        },
        versions_seen: {
          __input__: {},
          __start__: { __start__: 1 },
          node: { "start:node": 2 }
        },
        pending_sends: []
      },
      metadata: {
        tester: "artillery",
        tag: `run-${Math.random().toString(36).substring(7)}`
      }
    };

    return done();
  },

  generateKeyValue: function (userContext, events, done) {
    const key = `key-${Math.floor(Math.random() * 100000)}`;
    const value = `value-${Math.random().toFixed(5)}`;
    userContext.vars.item = { key, value };
    return done();
  }
};
