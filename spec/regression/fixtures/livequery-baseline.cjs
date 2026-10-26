"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ParseLiveQueryServer = void 0;
var _tv = _interopRequireDefault(require("tv4"));
var _node = _interopRequireDefault(require("parse/node"));
var _Subscription = require("./Subscription");
var _Client = require("./Client");
var _ParseWebSocketServer = require("./ParseWebSocketServer");
var _logger = _interopRequireDefault(require("../logger"));
var _RequestSchema = _interopRequireDefault(require("./RequestSchema"));
var _QueryTools = require("./QueryTools");
var _ParsePubSub = require("./ParsePubSub");
var _SchemaController = _interopRequireDefault(require("../Controllers/SchemaController"));
var _lodash = _interopRequireDefault(require("lodash"));
var _uuid = require("uuid");
var _triggers = require("../triggers");
var _Auth = require("../Auth");
var _Controllers = require("../Controllers");
var _lruCache = require("lru-cache");
var _UsersRouter = _interopRequireDefault(require("../Routers/UsersRouter"));
var _DatabaseController = _interopRequireDefault(require("../Controllers/DatabaseController"));
var _util = require("util");
var _deepcopy = _interopRequireDefault(require("deepcopy"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { default: e }; }
class ParseLiveQueryServer {
  // className -> (queryHash -> subscription)

  // The subscriber we use to get object update from publisher

  constructor(server, config = {}, parseServerConfig = {}) {
    this.server = server;
    this.clients = new Map();
    this.subscriptions = new Map();
    this.config = config;
    config.appId = config.appId || _node.default.applicationId;
    config.masterKey = config.masterKey || _node.default.masterKey;

    // Store keys, convert obj to map
    const keyPairs = config.keyPairs || {};
    this.keyPairs = new Map();
    for (const key of Object.keys(keyPairs)) {
      this.keyPairs.set(key, keyPairs[key]);
    }
    _logger.default.verbose('Support key pairs', this.keyPairs);

    // Initialize Parse
    _node.default.Object.disableSingleInstance();
    const serverURL = config.serverURL || _node.default.serverURL;
    _node.default.serverURL = serverURL;
    _node.default.initialize(config.appId, _node.default.javaScriptKey, config.masterKey);

    // The cache controller is a proper cache controller
    // with access to User and Roles
    this.cacheController = (0, _Controllers.getCacheController)(parseServerConfig);
    config.cacheTimeout = config.cacheTimeout || 5 * 1000; // 5s

    // This auth cache stores the promises for each auth resolution.
    // The main benefit is to be able to reuse the same user / session token resolution.
    this.authCache = new _lruCache.LRUCache({
      max: 500,
      // 500 concurrent
      ttl: config.cacheTimeout
    });
    // Initialize websocket server
    this.parseWebSocketServer = new _ParseWebSocketServer.ParseWebSocketServer(server, parseWebsocket => this._onConnect(parseWebsocket), config);
    this.subscriber = _ParsePubSub.ParsePubSub.createSubscriber(config);
    if (!this.subscriber.connect) {
      this.connect();
    }
  }
  async connect() {
    if (this.subscriber.isOpen) {
      return;
    }
    if (typeof this.subscriber.connect === 'function') {
      await Promise.resolve(this.subscriber.connect());
    } else {
      this.subscriber.isOpen = true;
    }
    this._createSubscribers();
  }
  async shutdown() {
    if (this.subscriber.isOpen) {
      var _this$subscriber$clos, _this$subscriber;
      await Promise.all([...[...this.clients.values()].map(client => client.parseWebSocket.ws.close()), this.parseWebSocketServer.close(), ...Array.from(this.subscriber.subscriptions.keys()).map(key => this.subscriber.unsubscribe(key)), (_this$subscriber$clos = (_this$subscriber = this.subscriber).close) === null || _this$subscriber$clos === void 0 ? void 0 : _this$subscriber$clos.call(_this$subscriber)]);
    }
    this.subscriber.isOpen = false;
  }
  _createSubscribers() {
    const messageRecieved = (channel, messageStr) => {
      _logger.default.verbose('Subscribe message %j', messageStr);
      let message;
      try {
        message = JSON.parse(messageStr);
      } catch (e) {
        _logger.default.error('unable to parse message', messageStr, e);
        return;
      }
      if (channel === _node.default.applicationId + 'clearCache') {
        this._clearCachedRoles(message.userId);
        return;
      }
      this._inflateParseObject(message);
      if (channel === _node.default.applicationId + 'afterSave') {
        this._onAfterSave(message);
      } else if (channel === _node.default.applicationId + 'afterDelete') {
        this._onAfterDelete(message);
      } else {
        _logger.default.error('Get message %s from unknown channel %j', message, channel);
      }
    };
    this.subscriber.on('message', (channel, messageStr) => messageRecieved(channel, messageStr));
    for (const field of ['afterSave', 'afterDelete', 'clearCache']) {
      const channel = `${_node.default.applicationId}${field}`;
      this.subscriber.subscribe(channel, messageStr => messageRecieved(channel, messageStr));
    }
  }

  // Message is the JSON object from publisher. Message.currentParseObject is the ParseObject JSON after changes.
  // Message.originalParseObject is the original ParseObject JSON.
  _inflateParseObject(message) {
    // Inflate merged object
    const currentParseObject = message.currentParseObject;
    _UsersRouter.default.removeHiddenProperties(currentParseObject);
    let className = currentParseObject.className;
    let parseObject = new _node.default.Object(className);
    parseObject._finishFetch(currentParseObject);
    message.currentParseObject = parseObject;
    // Inflate original object
    const originalParseObject = message.originalParseObject;
    if (originalParseObject) {
      _UsersRouter.default.removeHiddenProperties(originalParseObject);
      className = originalParseObject.className;
      parseObject = new _node.default.Object(className);
      parseObject._finishFetch(originalParseObject);
      message.originalParseObject = parseObject;
    }
  }

  // Message is the JSON object from publisher after inflated. Message.currentParseObject is the ParseObject after changes.
  // Message.originalParseObject is the original ParseObject.
  async _onAfterDelete(message) {
    _logger.default.verbose(_node.default.applicationId + 'afterDelete is triggered');
    let deletedParseObject = message.currentParseObject.toJSON();
    const classLevelPermissions = message.classLevelPermissions;
    const className = deletedParseObject.className;
    _logger.default.verbose('ClassName: %j | ObjectId: %s', className, deletedParseObject.id);
    _logger.default.verbose('Current client number : %d', this.clients.size);
    const classSubscriptions = this.subscriptions.get(className);
    if (typeof classSubscriptions === 'undefined') {
      _logger.default.debug('Can not find subscriptions under this class ' + className);
      return;
    }
    for (const subscription of classSubscriptions.values()) {
      let isSubscriptionMatched;
      try {
        isSubscriptionMatched = this._matchesSubscription(deletedParseObject, subscription);
      } catch (e) {
        _logger.default.error(`Failed matching subscription for class ${className}: ${e.message}`);
        continue;
      }
      if (!isSubscriptionMatched) {
        continue;
      }
      for (const [clientId, requestIds] of _lodash.default.entries(subscription.clientRequestIds)) {
        const client = this.clients.get(clientId);
        if (typeof client === 'undefined') {
          continue;
        }
        requestIds.forEach(async requestId => {
          // Deep-clone shared object so each concurrent callback works on its own copy
          let localDeletedParseObject = JSON.parse(JSON.stringify(deletedParseObject));
          const acl = message.currentParseObject.getACL();
          // Check CLP
          const op = this._getCLPOperation(subscription.query);
          let res = {};
          try {
            const matchesCLP = await this._matchesCLP(classLevelPermissions, message.currentParseObject, client, requestId, op);
            if (matchesCLP === false) {
              return null;
            }
            const isMatched = await this._matchesACL(acl, client, requestId);
            if (!isMatched) {
              return null;
            }
            res = {
              event: 'delete',
              sessionToken: client.sessionToken,
              object: localDeletedParseObject,
              clients: this.clients.size,
              subscriptions: this.subscriptions.size,
              useMasterKey: client.hasMasterKey,
              installationId: client.installationId,
              sendEvent: true
            };
            const trigger = (0, _triggers.getTrigger)(className, 'afterEvent', _node.default.applicationId);
            if (trigger) {
              const auth = await this.getAuthFromClient(client, requestId);
              if (auth && auth.user) {
                res.user = auth.user;
              }
              if (res.object) {
                res.object = _node.default.Object.fromJSON(res.object);
              }
              await (0, _triggers.runTrigger)(trigger, `afterEvent.${className}`, res, auth);
            }
            if (!res.sendEvent) {
              return;
            }
            if (res.object && typeof res.object.toJSON === 'function') {
              localDeletedParseObject = (0, _triggers.toJSONwithObjects)(res.object, res.object.className || className);
            }
            res.object = localDeletedParseObject;
            await this._filterSensitiveData(classLevelPermissions, res, client, requestId, op, subscription.query);
            client.pushDelete(requestId, res.object);
          } catch (e) {
            const error = (0, _triggers.resolveError)(e);
            _Client.Client.pushError(client.parseWebSocket, error.code, error.message, false, requestId);
            _logger.default.error(`Failed running afterLiveQueryEvent on class ${className} for event ${res.event} with session ${res.sessionToken} with:\n Error: ` + JSON.stringify(error));
          }
        });
      }
    }
  }

  // Message is the JSON object from publisher after inflated. Message.currentParseObject is the ParseObject after changes.
  // Message.originalParseObject is the original ParseObject.
  async _onAfterSave(message) {
    _logger.default.verbose(_node.default.applicationId + 'afterSave is triggered');
    let originalParseObject = null;
    if (message.originalParseObject) {
      originalParseObject = message.originalParseObject.toJSON();
    }
    const classLevelPermissions = message.classLevelPermissions;
    let currentParseObject = message.currentParseObject.toJSON();
    const className = currentParseObject.className;
    _logger.default.verbose('ClassName: %s | ObjectId: %s', className, currentParseObject.id);
    _logger.default.verbose('Current client number : %d', this.clients.size);
    const classSubscriptions = this.subscriptions.get(className);
    if (typeof classSubscriptions === 'undefined') {
      _logger.default.debug('Can not find subscriptions under this class ' + className);
      return;
    }
    for (const subscription of classSubscriptions.values()) {
      let isOriginalSubscriptionMatched;
      let isCurrentSubscriptionMatched;
      try {
        isOriginalSubscriptionMatched = this._matchesSubscription(originalParseObject, subscription);
        isCurrentSubscriptionMatched = this._matchesSubscription(currentParseObject, subscription);
      } catch (e) {
        _logger.default.error(`Failed matching subscription for class ${className}: ${e.message}`);
        continue;
      }
      for (const [clientId, requestIds] of _lodash.default.entries(subscription.clientRequestIds)) {
        const client = this.clients.get(clientId);
        if (typeof client === 'undefined') {
          continue;
        }
        requestIds.forEach(async requestId => {
          // Deep-clone shared objects so each concurrent callback works on its own copy.
          // Without cloning, _filterSensitiveData's in-place field deletion and afterEvent
          // trigger modifications corrupt the shared state across concurrent subscribers.
          let localCurrentParseObject = JSON.parse(JSON.stringify(currentParseObject));
          let localOriginalParseObject = originalParseObject ? JSON.parse(JSON.stringify(originalParseObject)) : null;
          // Set orignal ParseObject ACL checking promise, if the object does not match
          // subscription, we do not need to check ACL
          let originalACLCheckingPromise;
          if (!isOriginalSubscriptionMatched) {
            originalACLCheckingPromise = Promise.resolve(false);
          } else {
            let originalACL;
            if (message.originalParseObject) {
              originalACL = message.originalParseObject.getACL();
            }
            originalACLCheckingPromise = this._matchesACL(originalACL, client, requestId);
          }
          // Set current ParseObject ACL checking promise, if the object does not match
          // subscription, we do not need to check ACL
          let currentACLCheckingPromise;
          let res = {};
          if (!isCurrentSubscriptionMatched) {
            currentACLCheckingPromise = Promise.resolve(false);
          } else {
            const currentACL = message.currentParseObject.getACL();
            currentACLCheckingPromise = this._matchesACL(currentACL, client, requestId);
          }
          try {
            const op = this._getCLPOperation(subscription.query);
            const matchesCLP = await this._matchesCLP(classLevelPermissions, message.currentParseObject, client, requestId, op);
            if (matchesCLP === false) {
              return;
            }
            const [isOriginalMatched, isCurrentMatched] = await Promise.all([originalACLCheckingPromise, currentACLCheckingPromise]);
            _logger.default.verbose('Original %j | Current %j | Match: %s, %s, %s, %s | Query: %s', localOriginalParseObject, localCurrentParseObject, isOriginalSubscriptionMatched, isCurrentSubscriptionMatched, isOriginalMatched, isCurrentMatched, subscription.hash);
            // Decide event type
            let type;
            if (isOriginalMatched && isCurrentMatched) {
              type = 'update';
            } else if (isOriginalMatched && !isCurrentMatched) {
              type = 'leave';
            } else if (!isOriginalMatched && isCurrentMatched) {
              if (localOriginalParseObject) {
                type = 'enter';
              } else {
                type = 'create';
              }
            } else {
              return null;
            }
            const watchFieldsChanged = this._checkWatchFields(client, requestId, message);
            if (!watchFieldsChanged && (type === 'update' || type === 'create')) {
              return;
            }
            // A `leave` or `enter` transition can be caused either by the object's
            // query match changing (the subscriber keeps read access) or by the
            // subscriber's ACL read access being revoked or granted in the same save.
            // In the access-change case the subscriber is not authorized to read the
            // object state that triggered the transition, so that state must not be
            // sent over the channel. (CLP read denial is handled earlier by
            // `_matchesCLP`, which skips the event entirely.)
            if (type === 'leave') {
              // The post-update object is readable on a query-mismatch leave but not
              // on an ACL-loss leave. Only send the post-update body when the
              // subscriber can still read the current object; otherwise fall back to
              // the last authorized (original) state, which still carries the objectId.
              const currentReadable = isCurrentSubscriptionMatched ? false : await this._matchesACL(message.currentParseObject.getACL(), client, requestId);
              if (!currentReadable) {
                localCurrentParseObject = JSON.parse(JSON.stringify(localOriginalParseObject));
              }
            } else if (type === 'enter') {
              // The pre-update object was readable on a query-match-gain enter but not
              // on an ACL-grant enter. Only send the pre-update body as `original`
              // when the subscriber could read the original object.
              const originalReadable = isOriginalSubscriptionMatched ? false : await this._matchesACL(message.originalParseObject.getACL(), client, requestId);
              if (!originalReadable) {
                localOriginalParseObject = null;
              }
            }
            res = {
              event: type,
              sessionToken: client.sessionToken,
              object: localCurrentParseObject,
              original: localOriginalParseObject,
              clients: this.clients.size,
              subscriptions: this.subscriptions.size,
              useMasterKey: client.hasMasterKey,
              installationId: client.installationId,
              sendEvent: true
            };
            const trigger = (0, _triggers.getTrigger)(className, 'afterEvent', _node.default.applicationId);
            if (trigger) {
              if (res.object) {
                res.object = _node.default.Object.fromJSON(res.object);
              }
              if (res.original) {
                res.original = _node.default.Object.fromJSON(res.original);
              }
              const auth = await this.getAuthFromClient(client, requestId);
              if (auth && auth.user) {
                res.user = auth.user;
              }
              await (0, _triggers.runTrigger)(trigger, `afterEvent.${className}`, res, auth);
            }
            if (!res.sendEvent) {
              return;
            }
            if (res.object && typeof res.object.toJSON === 'function') {
              localCurrentParseObject = (0, _triggers.toJSONwithObjects)(res.object, res.object.className || className);
            }
            if (res.original && typeof res.original.toJSON === 'function') {
              localOriginalParseObject = (0, _triggers.toJSONwithObjects)(res.original, res.original.className || className);
            }
            res.object = localCurrentParseObject;
            res.original = localOriginalParseObject;
            await this._filterSensitiveData(classLevelPermissions, res, client, requestId, op, subscription.query);
            const functionName = 'push' + res.event.charAt(0).toUpperCase() + res.event.slice(1);
            if (client[functionName]) {
              client[functionName](requestId, res.object, res.original ?? null);
            }
          } catch (e) {
            const error = (0, _triggers.resolveError)(e);
            _Client.Client.pushError(client.parseWebSocket, error.code, error.message, false, requestId);
            _logger.default.error(`Failed running afterLiveQueryEvent on class ${className} for event ${res.event} with session ${res.sessionToken} with:\n Error: ` + JSON.stringify(error));
          }
        });
      }
    }
  }
  _onConnect(parseWebsocket) {
    parseWebsocket.on('message', request => {
      if (typeof request === 'string') {
        try {
          request = JSON.parse(request);
        } catch (e) {
          _logger.default.error('unable to parse request', request, e);
          return;
        }
      }
      _logger.default.verbose('Request: %j', request);

      // Check whether this request is a valid request, return error directly if not
      if (!_tv.default.validate(request, _RequestSchema.default['general']) || !_tv.default.validate(request, _RequestSchema.default[request.op])) {
        _Client.Client.pushError(parseWebsocket, 1, _tv.default.error.message);
        _logger.default.error('Connect message error %s', _tv.default.error.message);
        return;
      }
      switch (request.op) {
        case 'connect':
          this._handleConnect(parseWebsocket, request);
          break;
        case 'subscribe':
          this._handleSubscribe(parseWebsocket, request);
          break;
        case 'update':
          this._handleUpdateSubscription(parseWebsocket, request);
          break;
        case 'unsubscribe':
          this._handleUnsubscribe(parseWebsocket, request);
          break;
        default:
          _Client.Client.pushError(parseWebsocket, 3, 'Get unknown operation');
          _logger.default.error('Get unknown operation', request.op);
      }
    });
    parseWebsocket.on('disconnect', () => {
      _logger.default.info(`Client disconnect: ${parseWebsocket.clientId}`);
      const clientId = parseWebsocket.clientId;
      if (!this.clients.has(clientId)) {
        (0, _triggers.runLiveQueryEventHandlers)({
          event: 'ws_disconnect_error',
          clients: this.clients.size,
          subscriptions: this.subscriptions.size,
          error: `Unable to find client ${clientId}`
        });
        _logger.default.error(`Can not find client ${clientId} on disconnect`);
        return;
      }

      // Delete client
      const client = this.clients.get(clientId);
      this.clients.delete(clientId);

      // Delete client from subscriptions
      for (const [requestId, subscriptionInfo] of _lodash.default.entries(client.subscriptionInfos)) {
        const subscription = subscriptionInfo.subscription;
        subscription.deleteClientSubscription(clientId, requestId);

        // If there is no client which is subscribing this subscription, remove it from subscriptions
        const classSubscriptions = this.subscriptions.get(subscription.className);
        if (!subscription.hasSubscribingClient()) {
          classSubscriptions.delete(subscription.hash);
        }
        // If there is no subscriptions under this class, remove it from subscriptions
        if (classSubscriptions.size === 0) {
          this.subscriptions.delete(subscription.className);
        }
      }
      _logger.default.verbose('Current clients %d', this.clients.size);
      _logger.default.verbose('Current subscriptions %d', this.subscriptions.size);
      (0, _triggers.runLiveQueryEventHandlers)({
        event: 'ws_disconnect',
        clients: this.clients.size,
        subscriptions: this.subscriptions.size,
        useMasterKey: client.hasMasterKey,
        installationId: client.installationId,
        sessionToken: client.sessionToken
      });
    });
    (0, _triggers.runLiveQueryEventHandlers)({
      event: 'ws_connect',
      clients: this.clients.size,
      subscriptions: this.subscriptions.size
    });
  }
  _matchesSubscription(parseObject, subscription) {
    // Object is undefined or null, not match
    if (!parseObject) {
      return false;
    }
    return (0, _QueryTools.matchesQuery)((0, _deepcopy.default)(parseObject), subscription.query);
  }
  async _clearCachedRoles(userId) {
    try {
      const validTokens = await new _node.default.Query(_node.default.Session).equalTo('user', _node.default.User.createWithoutData(userId)).find({
        useMasterKey: true
      });
      await Promise.all(validTokens.map(async token => {
        var _auth1$auth, _auth2$auth;
        const sessionToken = token.get('sessionToken');
        const authPromise = this.authCache.get(sessionToken);
        if (!authPromise) {
          return;
        }
        const [auth1, auth2] = await Promise.all([authPromise, (0, _Auth.getAuthForSessionToken)({
          cacheController: this.cacheController,
          sessionToken
        })]);
        (_auth1$auth = auth1.auth) === null || _auth1$auth === void 0 || _auth1$auth.clearRoleCache(sessionToken);
        (_auth2$auth = auth2.auth) === null || _auth2$auth === void 0 || _auth2$auth.clearRoleCache(sessionToken);
        this.authCache.delete(sessionToken);
      }));
    } catch (e) {
      _logger.default.verbose(`Could not clear role cache. ${e}`);
    }
  }
  getAuthForSessionToken(sessionToken) {
    if (!sessionToken) {
      return Promise.resolve({});
    }
    const fromCache = this.authCache.get(sessionToken);
    if (fromCache) {
      return fromCache;
    }
    const authPromise = (0, _Auth.getAuthForSessionToken)({
      cacheController: this.cacheController,
      sessionToken: sessionToken
    }).then(auth => {
      return {
        auth,
        userId: auth && auth.user && auth.user.id
      };
    }).catch(error => {
      // There was an error with the session token
      const result = {};
      if (error && error.code === _node.default.Error.INVALID_SESSION_TOKEN) {
        result.error = error;
        this.authCache.set(sessionToken, Promise.resolve(result), this.config.cacheTimeout);
      } else {
        this.authCache.delete(sessionToken);
      }
      return result;
    });
    this.authCache.set(sessionToken, authPromise);
    return authPromise;
  }
  async _matchesCLP(classLevelPermissions, object, client, requestId, op) {
    // try to match on user first, less expensive than with roles
    const subscriptionInfo = client.getSubscriptionInfo(requestId);
    const aclGroup = ['*'];
    let userId;
    if (typeof subscriptionInfo !== 'undefined') {
      const {
        userId
      } = await this.getAuthForSessionToken(subscriptionInfo.sessionToken);
      if (userId) {
        aclGroup.push(userId);
      }
    }
    try {
      await _SchemaController.default.validatePermission(classLevelPermissions, object.className, aclGroup, op);
      return true;
    } catch (e) {
      _logger.default.verbose(`Failed matching CLP for ${object.id} ${userId} ${e}`);
      return false;
    }
    // TODO: handle roles permissions
    // Object.keys(classLevelPermissions).forEach((key) => {
    //   const perm = classLevelPermissions[key];
    //   Object.keys(perm).forEach((key) => {
    //     if (key.indexOf('role'))
    //   });
    // })
    // // it's rejected here, check the roles
    // var rolesQuery = new Parse.Query(Parse.Role);
    // rolesQuery.equalTo("users", user);
    // return rolesQuery.find({useMasterKey:true});
  }
  async _filterSensitiveData(classLevelPermissions, res, client, requestId, op, query) {
    const subscriptionInfo = client.getSubscriptionInfo(requestId);
    const aclGroup = ['*'];
    let clientAuth;
    if (typeof subscriptionInfo !== 'undefined') {
      const {
        userId,
        auth
      } = await this.getAuthForSessionToken(subscriptionInfo.sessionToken);
      if (userId) {
        aclGroup.push(userId);
      }
      clientAuth = auth;
    }
    const filter = obj => {
      if (!obj) {
        return;
      }
      let protectedFields = (classLevelPermissions === null || classLevelPermissions === void 0 ? void 0 : classLevelPermissions.protectedFields) || [];
      if (!client.hasMasterKey && !Array.isArray(protectedFields)) {
        // Parse 7.5.4's method has no controller state or database access.
        // Recompute the policy for every recipient; only avoid constructing the adapter.
        protectedFields = _DatabaseController.default.prototype.addProtectedFields.call(null, classLevelPermissions, res.object.className, query, aclGroup, clientAuth);
      }
      return _DatabaseController.default.filterSensitiveData(client.hasMasterKey, false, aclGroup, clientAuth, op, classLevelPermissions, res.object.className, protectedFields, obj, query);
    };
    res.object = filter(res.object);
    res.original = filter(res.original);
  }
  _getCLPOperation(query) {
    return typeof query === 'object' && Object.keys(query).length == 1 && typeof query.objectId === 'string' ? 'get' : 'find';
  }
  async _verifyACL(acl, token) {
    if (!token) {
      return false;
    }
    const {
      auth,
      userId
    } = await this.getAuthForSessionToken(token);

    // Getting the session token failed
    // This means that no additional auth is available
    // At this point, just bail out as no additional visibility can be inferred.
    if (!auth || !userId) {
      return false;
    }
    const isSubscriptionSessionTokenMatched = acl.getReadAccess(userId);
    if (isSubscriptionSessionTokenMatched) {
      return true;
    }

    // Check if the user has any roles that match the ACL
    return Promise.resolve().then(async () => {
      // Resolve false right away if the acl doesn't have any roles
      const acl_has_roles = Object.keys(acl.permissionsById).some(key => key.startsWith('role:'));
      if (!acl_has_roles) {
        return false;
      }
      const roleNames = await auth.getUserRoles();
      // Finally, see if any of the user's roles allow them read access
      for (const role of roleNames) {
        // We use getReadAccess as `role` is in the form `role:roleName`
        if (acl.getReadAccess(role)) {
          return true;
        }
      }
      return false;
    }).catch(() => {
      return false;
    });
  }
  async getAuthFromClient(client, requestId, sessionToken) {
    const getSessionFromClient = () => {
      const subscriptionInfo = client.getSubscriptionInfo(requestId);
      if (typeof subscriptionInfo === 'undefined') {
        return client.sessionToken;
      }
      return subscriptionInfo.sessionToken || client.sessionToken;
    };
    if (!sessionToken) {
      sessionToken = getSessionFromClient();
    }
    if (!sessionToken) {
      return;
    }
    const {
      auth
    } = await this.getAuthForSessionToken(sessionToken);
    return auth;
  }
  _checkWatchFields(client, requestId, message) {
    const subscriptionInfo = client.getSubscriptionInfo(requestId);
    const watch = subscriptionInfo === null || subscriptionInfo === void 0 ? void 0 : subscriptionInfo.watch;
    if (!watch) {
      return true;
    }
    const object = message.currentParseObject;
    const original = message.originalParseObject;
    return watch.some(field => !(0, _util.isDeepStrictEqual)(object.get(field), original === null || original === void 0 ? void 0 : original.get(field)));
  }
  async _matchesACL(acl, client, requestId) {
    // Return true directly if ACL isn't present, ACL is public read, or client has master key
    if (!acl || acl.getPublicReadAccess() || client.hasMasterKey) {
      return true;
    }
    // Check subscription sessionToken matches ACL first
    const subscriptionInfo = client.getSubscriptionInfo(requestId);
    if (typeof subscriptionInfo === 'undefined') {
      return false;
    }
    const subscriptionToken = subscriptionInfo.sessionToken;
    const clientSessionToken = client.sessionToken;
    if (await this._verifyACL(acl, subscriptionToken)) {
      return true;
    }
    if (await this._verifyACL(acl, clientSessionToken)) {
      return true;
    }
    return false;
  }
  async _handleConnect(parseWebsocket, request) {
    if (!this._validateKeys(request, this.keyPairs)) {
      _Client.Client.pushError(parseWebsocket, 4, 'Key in request is not valid');
      _logger.default.error('Key in request is not valid');
      return;
    }
    const hasMasterKey = this._hasMasterKey(request, this.keyPairs);
    const clientId = (0, _uuid.v4)();
    const client = new _Client.Client(clientId, parseWebsocket, hasMasterKey, request.sessionToken, request.installationId);
    try {
      const req = {
        client,
        event: 'connect',
        clients: this.clients.size,
        subscriptions: this.subscriptions.size,
        sessionToken: request.sessionToken,
        useMasterKey: client.hasMasterKey,
        installationId: request.installationId
      };
      const trigger = (0, _triggers.getTrigger)('@Connect', 'beforeConnect', _node.default.applicationId);
      if (trigger) {
        const auth = await this.getAuthFromClient(client, request.requestId, req.sessionToken);
        if (auth && auth.user) {
          req.user = auth.user;
        }
        await (0, _triggers.runTrigger)(trigger, `beforeConnect.@Connect`, req, auth);
      }
      parseWebsocket.clientId = clientId;
      this.clients.set(parseWebsocket.clientId, client);
      _logger.default.info(`Create new client: ${parseWebsocket.clientId}`);
      client.pushConnect();
      (0, _triggers.runLiveQueryEventHandlers)(req);
    } catch (e) {
      const error = (0, _triggers.resolveError)(e);
      _Client.Client.pushError(parseWebsocket, error.code, error.message, false);
      _logger.default.error(`Failed running beforeConnect for session ${request.sessionToken} with:\n Error: ` + JSON.stringify(error));
    }
  }
  _hasMasterKey(request, validKeyPairs) {
    if (!validKeyPairs || validKeyPairs.size == 0 || !validKeyPairs.has('masterKey')) {
      return false;
    }
    if (!request || !Object.prototype.hasOwnProperty.call(request, 'masterKey')) {
      return false;
    }
    return request.masterKey === validKeyPairs.get('masterKey');
  }
  _validateKeys(request, validKeyPairs) {
    if (!validKeyPairs || validKeyPairs.size == 0) {
      return true;
    }
    let isValid = false;
    for (const [key, secret] of validKeyPairs) {
      if (!request[key] || request[key] !== secret) {
        continue;
      }
      isValid = true;
      break;
    }
    return isValid;
  }
  async _handleSubscribe(parseWebsocket, request) {
    // If we can not find this client, return error to client
    if (!Object.prototype.hasOwnProperty.call(parseWebsocket, 'clientId')) {
      _Client.Client.pushError(parseWebsocket, 2, 'Can not find this client, make sure you connect to server before subscribing');
      _logger.default.error('Can not find this client, make sure you connect to server before subscribing');
      return;
    }
    const client = this.clients.get(parseWebsocket.clientId);
    const className = request.query.className;
    let authCalled = false;
    try {
      const trigger = (0, _triggers.getTrigger)(className, 'beforeSubscribe', _node.default.applicationId);
      if (trigger) {
        const auth = await this.getAuthFromClient(client, request.requestId, request.sessionToken);
        authCalled = true;
        if (auth && auth.user) {
          request.user = auth.user;
        }
        const parseQuery = new _node.default.Query(className);
        parseQuery.withJSON(request.query);
        request.query = parseQuery;
        await (0, _triggers.runTrigger)(trigger, `beforeSubscribe.${className}`, request, auth);
        const query = request.query.toJSON();
        request.query = query;
      }
      if (className === '_Session') {
        if (!authCalled) {
          const auth = await this.getAuthFromClient(client, request.requestId, request.sessionToken);
          if (auth && auth.user) {
            request.user = auth.user;
          }
        }
        if (request.user) {
          request.query.where.user = request.user.toPointer();
        } else if (!request.master) {
          _Client.Client.pushError(parseWebsocket, _node.default.Error.INVALID_SESSION_TOKEN, 'Invalid session token', false, request.requestId);
          return;
        }
      }
      // Get subscription from subscriptions, create one if necessary
      const subscriptionHash = (0, _QueryTools.queryHash)(request.query);
      // Add className to subscriptions if necessary

      if (!this.subscriptions.has(className)) {
        this.subscriptions.set(className, new Map());
      }
      const classSubscriptions = this.subscriptions.get(className);
      let subscription;
      if (classSubscriptions.has(subscriptionHash)) {
        subscription = classSubscriptions.get(subscriptionHash);
      } else {
        subscription = new _Subscription.Subscription(className, request.query.where, subscriptionHash);
        classSubscriptions.set(subscriptionHash, subscription);
      }

      // Add subscriptionInfo to client
      const subscriptionInfo = {
        subscription: subscription
      };
      // Add selected fields, sessionToken and installationId for this subscription if necessary
      if (request.query.keys) {
        subscriptionInfo.keys = Array.isArray(request.query.keys) ? request.query.keys : request.query.keys.split(',');
      }
      if (request.query.watch) {
        subscriptionInfo.watch = request.query.watch;
      }
      if (request.sessionToken) {
        subscriptionInfo.sessionToken = request.sessionToken;
      }
      client.addSubscriptionInfo(request.requestId, subscriptionInfo);

      // Add clientId to subscription
      subscription.addClientSubscription(parseWebsocket.clientId, request.requestId);
      client.pushSubscribe(request.requestId);
      _logger.default.verbose(`Create client ${parseWebsocket.clientId} new subscription: ${request.requestId}`);
      _logger.default.verbose('Current client number: %d', this.clients.size);
      (0, _triggers.runLiveQueryEventHandlers)({
        client,
        event: 'subscribe',
        clients: this.clients.size,
        subscriptions: this.subscriptions.size,
        sessionToken: request.sessionToken,
        useMasterKey: client.hasMasterKey,
        installationId: client.installationId
      });
    } catch (e) {
      const error = (0, _triggers.resolveError)(e);
      _Client.Client.pushError(parseWebsocket, error.code, error.message, false, request.requestId);
      _logger.default.error(`Failed running beforeSubscribe on ${className} for session ${request.sessionToken} with:\n Error: ` + JSON.stringify(error));
    }
  }
  _handleUpdateSubscription(parseWebsocket, request) {
    this._handleUnsubscribe(parseWebsocket, request, false);
    this._handleSubscribe(parseWebsocket, request);
  }
  _handleUnsubscribe(parseWebsocket, request, notifyClient = true) {
    // If we can not find this client, return error to client
    if (!Object.prototype.hasOwnProperty.call(parseWebsocket, 'clientId')) {
      _Client.Client.pushError(parseWebsocket, 2, 'Can not find this client, make sure you connect to server before unsubscribing');
      _logger.default.error('Can not find this client, make sure you connect to server before unsubscribing');
      return;
    }
    const requestId = request.requestId;
    const client = this.clients.get(parseWebsocket.clientId);
    if (typeof client === 'undefined') {
      _Client.Client.pushError(parseWebsocket, 2, 'Cannot find client with clientId ' + parseWebsocket.clientId + '. Make sure you connect to live query server before unsubscribing.');
      _logger.default.error('Can not find this client ' + parseWebsocket.clientId);
      return;
    }
    const subscriptionInfo = client.getSubscriptionInfo(requestId);
    if (typeof subscriptionInfo === 'undefined') {
      _Client.Client.pushError(parseWebsocket, 2, 'Cannot find subscription with clientId ' + parseWebsocket.clientId + ' subscriptionId ' + requestId + '. Make sure you subscribe to live query server before unsubscribing.');
      _logger.default.error('Can not find subscription with clientId ' + parseWebsocket.clientId + ' subscriptionId ' + requestId);
      return;
    }

    // Remove subscription from client
    client.deleteSubscriptionInfo(requestId);
    // Remove client from subscription
    const subscription = subscriptionInfo.subscription;
    const className = subscription.className;
    subscription.deleteClientSubscription(parseWebsocket.clientId, requestId);
    // If there is no client which is subscribing this subscription, remove it from subscriptions
    const classSubscriptions = this.subscriptions.get(className);
    if (!subscription.hasSubscribingClient()) {
      classSubscriptions.delete(subscription.hash);
    }
    // If there is no subscriptions under this class, remove it from subscriptions
    if (classSubscriptions.size === 0) {
      this.subscriptions.delete(className);
    }
    (0, _triggers.runLiveQueryEventHandlers)({
      client,
      event: 'unsubscribe',
      clients: this.clients.size,
      subscriptions: this.subscriptions.size,
      sessionToken: subscriptionInfo.sessionToken,
      useMasterKey: client.hasMasterKey,
      installationId: client.installationId
    });
    if (!notifyClient) {
      return;
    }
    client.pushUnsubscribe(request.requestId);
    _logger.default.verbose(`Delete client: ${parseWebsocket.clientId} | subscription: ${request.requestId}`);
  }
}
exports.ParseLiveQueryServer = ParseLiveQueryServer;
