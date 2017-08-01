'use strict';

const assert   = require('assert');
const pg       = require('co-pg')(require('pg'));
const debug    = require('debug')('shiba:db');
const debugpg  = require('debug')('verbose:db:pg');

const Cache  = require('./Util/Cache');
const Config = require('./Config');
const Lib    = require('./Lib');

pg.defaults.poolSize        = 3;
pg.defaults.poolIdleTimeout = 500000;

pg.types.setTypeParser(20, function(val) { // parse int8 as an integer
  return val === null ? null : parseInt(val);
});

let querySeq = 0;
function *query(sql, params) {
  let qid = querySeq++;
  debugpg("[%d] Executing query '%s'", qid, sql);
  if (params) debugpg("[%d] Parameters %s", qid, JSON.stringify(params));

  let vals   = yield pg.connectPromise(Config.DATABASE);
  let client = vals[0];
  let done   = vals[1];

  try {
    let result = yield client.queryPromise(sql, params);
    debugpg("[%d] Finished query", qid);
    return result;
  } finally {
    // Release client back to pool
    done();
  }
}

/**
 * Runs a session and retries if it deadlocks.
 *
 * @param {String} runner A generator expecting a query function.
 * @return Session result.
 * @api private
 */
function* withClient(runner) {

  let vals   = yield pg.connectPromise(Config.DATABASE);
  let client = vals[0];
  let done   = vals[1];

  let query =
        function*(sql,params) {
          let qid = querySeq++;
          debugpg("[%d] Executing query '%s'", qid, sql);
          if (params) debugpg("[%d] Parameters %s", qid, JSON.stringify(params));

          let result = yield client.queryPromise(sql, params);
          debugpg("[%d] Finished query", qid);
          return result;
        };

  try {
    let result = yield* runner(query);
    done();
    return result;
  } catch (ex) {
    if (ex.code === '40P01') { // Deadlock
      done();
      return yield* withClient(runner);
    }

    console.error(ex);
    console.error(ex.stack);

    if (ex.removeFromPool) {
      console.error('[ERROR] withClient: removing connection from pool');
      done(new Error('Removing connection from pool'));
    } else {
      done();
    }
    throw ex;
  }
}

let txSeq = 0;

/**
 * Runs a single transaction and retry if it deadlocks. This function
 * takes care of BEGIN and COMMIT. The session runner should never
 * perform these queries itself.
 *
 * @param {String} runner A generator expecting a query function.
 * @return Transaction result.
 * @api private
 */
function* withTransaction(runner) {

  return yield* withClient(function*(query) {
    let txid = txSeq++;
    try {
      debugpg('[%d] Starting transaction', txid);
      yield* query('BEGIN');
      let result = yield* runner(query);
      debugpg('[%d] Committing transaction', txid);
      yield* query('COMMIT');
      debugpg('[%d] Finished transaction', txid);
      return result;
    } catch (ex) {
      try {
        yield* query('ROLLBACK');
      } catch(ex) {
        ex.removeFromPool = true;
        throw ex;
      }
      throw ex;
    }
  });
}

function* getOrCreateUser(username) {
  debug('GetOrCreateUser user: ' + username);

  return yield* withTransaction(function*(query) {
    let sql = 'SELECT * FROM users WHERE lower(username) = lower($1)';
    let par = [username];

    let result = yield* query(sql, par);

    if (result.rows.length > 0) {
      // User exists. Return the first (and only) row.
      assert(result.rows.length === 1);
      return result.rows[0];
    }

    // Create new user.
    sql = 'INSERT INTO users(username) VALUES($1) RETURNING username, id';
    par = [username];

    result = yield* query(sql, par);
    assert(result.rows.length === 1);
    return result.rows[0];
  });
}

function* getExistingUser(username) {
  debug('Getting user: ' + username);

  if (Lib.isInvalidUsername(username))
    throw 'USERNAME_INVALID';

  let sql = 'SELECT * FROM users WHERE lower(username) = lower($1)';
  let par = [username];

  let data = yield* query(sql, par);

  if (data.rows.length > 0) {
    // User exists. Return the first (and only) row.
    assert(data.rows.length === 1);
    return data.rows[0];
  } else {
    throw 'USER_DOES_NOT_EXIST';
  }
}

const userCache = new Cache({
  maxAge: 1000 * 60 * 10, // 10 minutes
  load : getOrCreateUser
});

function* getUser(username) {
  if (Lib.isInvalidUsername(username))
    throw 'USERNAME_INVALID';

  try {
    return yield* userCache.get(username);
  } catch(err) {
    console.error('[Pg.getUser] ERROR:', err);
    throw err;
  }
}

/*
CREATE TABLE chats (
  id bigint NOT NULL.
  user_id bigint NOT NULL,
  message text NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL
);
*/
exports.putChat = function*(username, message, timestamp, isBot, channelName) {
  debug('Recording chat message. User: ' + username);
  let user = yield* getUser(username);

  let sql = 'INSERT INTO chats(user_id, message, created) VALUES ($1, $2, $3)';
  let par = [user.id, message, timestamp];
  yield* query(sql, par);
};

/*
CREATE TABLE mutes (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  moderator_id bigint NOT NULL,
  timespec text NOT NULL,
  shadow boolean DEFAULT false NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL
);
*/
exports.putMute = function*(username, moderatorname, timespec, shadow, timestamp) {
  debug('Recording mute message.' +
        ' User: ' + username + '.' +
        ' Moderator: ' + moderatorname);

  let vals = yield [username,moderatorname].map(getUser),
      usr  = vals[0],
      mod  = vals[1],
      sql  =
        'INSERT INTO ' +
        'mutes(user_id, moderator_id, timespec, shadow, created) ' +
        'VALUES ($1, $2, $3, $4, $5)',
      par  = [usr.id, mod.id, timespec, shadow, timestamp];
  yield* query(sql, par);
};

exports.putUnmute = function*(username, moderatorname, shadow, timestamp) {
  debug('Recording unmute message.' +
        ' User: ' + username + '.' +
        ' Moderator: ' + moderatorname);

  let vals = yield [username,moderatorname].map(getUser);
  let usr  = vals[0];
  let mod  = vals[1];

  let sql =
    'INSERT INTO ' +
    'unmutes(user_id, moderator_id, shadow, created) ' +
    'VALUES ($1, $2, $3, $4)';
  let par = [usr.id, mod.id, shadow, timestamp];
  yield* query(sql, par);
};

exports.putMsg = function*(msg) {
  switch(msg.type) {
  case 'say':
    yield* this.putChat(msg.username, msg.message, new Date(msg.date));
    break;
  case 'mute':
    yield* this.putMute(
      msg.username, msg.moderator, msg.timespec,
      msg.shadow, new Date(msg.date));
    break;
  case 'unmute':
    yield* this.putUnmute(
      msg.username, msg.moderator, msg.shadow,
      new Date(msg.date));
    break;
  case 'error':
  case 'info':
    break;
  default:
    throw 'UNKNOWN_MSG_TYPE';
  }
};

exports.getLastMessages = function*() {
  debug('Retrieving last chat messages');

  let sql1 =
    "SELECT\
       chats.created AS date,\
       'say' AS type,\
       username,\
       message\
     FROM chats\
     JOIN users ON chats.user_id = users.id\
     ORDER BY date DESC\
     LIMIT $1";
  let sql2 =
    "SELECT\
       mutes.created AS date,\
       'mute' AS type,\
       m.username AS moderator,\
       u.username,\
       timespec,\
       shadow\
     FROM mutes\
     JOIN users AS m ON mutes.moderator_id = m.id\
     JOIN users AS u ON mutes.user_id = u.id\
     ORDER BY date DESC\
     LIMIT $1";
  let sql3 =
    "SELECT\
       unmutes.created AS date,\
       'unmute' AS type,\
       m.username AS moderator,\
       u.username,\
       shadow\
     FROM unmutes\
     JOIN users AS m ON unmutes.moderator_id = m.id\
     JOIN users AS u ON unmutes.user_id = u.id\
     ORDER BY date DESC\
     LIMIT $1";
  let par = [Config.CHAT_HISTORY];

  let res = yield [sql1,sql2,sql3].map((sql) => query(sql,par));
  res = res[0].rows.concat(res[1].rows,res[2].rows);
  res = res.sort((a,b) => new Date(a.date) - new Date(b.date));

  return res;
};

exports.putGame = function*(info) {
  let players = Object.keys(info.player_info);

  // Step1: Resolve all player names.
  let users   = yield players.map(getUser);
  let userIds = {};
  for (let i in users)
    userIds[users[i].username] = users[i].id;

  // Insert into the games and plays table in a common transaction.
  debug('Recording info for game #' + info.game_id);
  yield* withTransaction(function*(query) {
    debugpg('Inserting game data for game #' + info.game_id);

    let sql =
      'INSERT INTO ' +
      'games(id, game_crash, seed, started) ' +
      'VALUES ($1, $2, $3, $4)';
    let par =
      [ info.game_id,
        info.game_crash,
        info.server_seed || info.hash,
        info.started || null
      ];
    yield* query(sql, par);

    for (let player of players) {
      debugpg('Inserting play for ' + player);

      let play = info.player_info[player];
      let sql =
        'INSERT INTO ' +
        'plays(user_id, cash_out, game_id, bet, bonus, joined) ' +
        'VALUES ($1, $2, $3, $4, $5, $6)';
      let par =
        [ userIds[player],
          play.stopped_at ? Math.round(play.bet * play.stopped_at / 100) : null,
          info.game_id,
          play.bet,
          play.bonus || null,
          play.joined || null
        ];

      try {
        yield* query(sql, par);
      } catch(err) {
        console.error('Insert play failed. Values:', play);
        throw err;
      }
    }
  });
};

/*
CREATE TABLE licks (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  message text NOT NULL,
  creator_id bigint,
  created timestamp with time zone DEFAULT now() NOT NULL
);
*/
exports.putLick = function*(username, message, creatorname) {
  debug('Recording custom lick message for user: ' + username);

  let vals    = yield [username,creatorname].map(getUser);
  let user    = vals[0];
  let creator = vals[1];

  let sql =
    'INSERT INTO ' +
    'licks(user_id, message, creator_id) ' +
    'VALUES ($1, $2, $3)';
  let par = [user.id, message, creator.id];
  yield* query(sql, par);
};

exports.getLastGames = function*() {
  debug('Retrieving last games');

  let sql =
    'WITH t AS (SELECT\
                  id AS game_id,\
                  game_crash,\
                  created,\
                  seed AS hash\
                FROM games\
                ORDER BY id\
                DESC LIMIT $1)\
     SELECT * FROM t ORDER BY game_id';
  let par = [Config.GAME_HISTORY];

  let data = yield* query(sql,par);
  return data.rows;
};

exports.getLick = function*(username) {
  debug('Getting custom lick messages for user: ' + username);
  let user = yield* getExistingUser(username);

  let sql   = 'SELECT message FROM licks WHERE user_id = $1';
  let par   = [user.id];
  let data  = yield* query(sql, par);
  let licks = data.rows.map(row => row.message);

  return {username: user.username, licks: licks};
};

exports.getBust = function*(qry) {
  debug('Getting last bust: ' + JSON.stringify(qry));
  let sql, par;
  if (qry === 'MAX') {
    sql = 'SELECT * FROM games WHERE id =' +
          ' (SELECT id FROM game_crashes' +
          '   ORDER BY game_crash DESC LIMIT 1)';
    par = [];
  } else {
    let min   = qry.hasOwnProperty('min') ? ' AND game_crash >= ' + qry.min : '';
    let max   = qry.hasOwnProperty('max') ? ' AND game_crash <= ' + qry.max : '';
    let range = 'TRUE' + min + max;
    sql = 'SELECT * FROM games WHERE id =' +
          ' (SELECT id FROM game_crashes' +
          '   WHERE ' + range +
          '   ORDER BY id DESC LIMIT 1)';
    par = [];
  }

  try {
    let data = yield* query(sql, par);
    return data.rows;
  } catch(err) {
    console.error(err);
    throw err;
  }
};

exports.addAutomute = function*(creator, regex) {
  regex = regex.toString();
  debug('Adding automute ' + regex);

  let user = yield* getUser(creator);
  let sql  = 'INSERT INTO automutes(creator_id, regexp) VALUES($1, $2)';
  let par  = [user.id, regex];

  try {
    yield* query(sql, par);
  } catch(err) {
    console.error(err);
    throw err;
  }
};

exports.getAutomutes = function*() {
  debug('Getting automute list.');
  let sql = 'SELECT regexp FROM automutes WHERE enabled';
  let par = [];

  let data = yield* query(sql, par);

  let reg = /^\/(.*)\/([gi]*)$/;
  let res = data.rows.map(row => {
    let match = row.regexp.match(reg);
    return new RegExp(match[1], match[2]);
  });

  return res;
};

exports.getLastSeen = function*(username) {
  debug('Getting last chat message of user ' + username);

  let user = yield* getExistingUser(username);

  let sql =
    'SELECT created FROM chats WHERE user_id = $1 ' +
    'ORDER BY created DESC LIMIT 1';
  let par  = [user.id];
  let data = yield* query(sql, par);

  if (data.rows.length > 0) {
    // Return the first (and only) row.
    assert(data.rows.length === 1);
    return {
      username: user.username,
      time:     new Date(data.rows[0].created)
    };
  } else {
    // User never said anything.
    return {username: user.username};
  }
};

exports.getLatestBlock = function*() {
  console.log('Getting last block from DB');
  let sql = 'SELECT * FROM blocks ORDER BY height DESC LIMIT 1';
  let par = [];

  let data = yield* query(sql, par);
  return data.rows[0];
};

exports.putBlock = function*(block) {
  let sql = 'INSERT INTO blocks(height, hash) VALUES($1, $2)';
  let par = [block.height, block.hash];

  try {
    yield* query(sql, par);
  } catch(err) {
    // Ignore unique_violation code 23505.
    if (err.code === 23505 || err.code === '23505')
      debugpg('Block database entry already exists');
    else
      throw err;
  }
};

exports.getBlockNotifications = function*() {
  debug('Getting block notification list');
  let sql = 'SELECT channel_name, array_agg(username) AS users FROM blocknotifications GROUP BY channel_name';

  let par  = [];
  let data = yield* query(sql, par);

  let map = new Map();
  data.rows.forEach(function(val, index) {
    map.set(val['channel_name'], val['users']);
  });
  return map;
};

exports.putBlockNotification = function*(username, channelName) {
  debug('Adding %s to block notification list on channel %s', username, channelName);
  let sql = 'INSERT INTO blocknotifications(username, channel_name) VALUES($1, $2)';
  let par = [username, channelName];

  try {
    yield* query(sql, par);
  } catch(err) {
    // Ignore unique_violation code 23505.
    if (err.code !== 23505 && err.code !== '23505') throw err;
  }
};

exports.clearBlockNotifications = function*() {
  debug('Clearing block notification list');
  let sql = 'DELETE FROM blocknotifications';
  let par = [];
  yield* query(sql, par);
};

exports.getGameCrashMedian = function*(numGames) {
  debug('Retrieving game crash median');

  let sql =
    'WITH t AS (SELECT game_crash FROM games ORDER BY id DESC LIMIT $1)\
       SELECT percentile_cont(0.5)\
       WITHIN GROUP (ORDER BY game_crash) AS median, COUNT(*)\
       FROM t';
  let par = [numGames];
  let data = yield* query(sql, par);

  return data.rows[0];
};

exports.getLastStreak = function*(count, op, bound) {
  debug('Retrieving last streak');

  let sql =
    'WITH\
       t1 AS\
         (SELECT id, CASE WHEN id IS DISTINCT FROM (lag(id) OVER (ORDER BY id)) + 1 THEN id END AS id_start\
            FROM games\
            WHERE game_crash ' + op + ' $1),\
       t2 AS\
         (SELECT id, max(id_start) OVER (ORDER BY id) AS id_group\
            FROM t1),\
       best AS\
         (SELECT id_group, COUNT(*)\
            FROM t2\
            GROUP BY id_group\
            HAVING count(*) >= $2\
            ORDER BY id_group DESC LIMIT 1)\
     SELECT id game_id, game_crash game_crash\
     FROM games, best\
     WHERE id >= best.id_group AND id < best.id_group + best.count\
     ORDER BY id';

  let par = [bound, count];
  let data = yield* query(sql, par);

  return data.rows;
};

exports.getMaxStreak = function*(op, bound) {
  debug('Retrieving max streak');

  let sql =
    'WITH\
       t1 AS\
         (SELECT id, CASE WHEN id IS DISTINCT FROM (lag(id) OVER (ORDER BY id)) + 1 THEN id END AS id_start\
            FROM games\
            WHERE game_crash ' + op + ' $1),\
       t2 AS\
         (SELECT id, max(id_start) OVER (ORDER BY id) AS id_group\
            FROM t1),\
       best AS\
         (SELECT id_group, COUNT(*) AS count\
            FROM t2\
            GROUP BY id_group\
            ORDER BY count DESC LIMIT 1)\
     SELECT id game_id, game_crash game_crash\
     FROM games, best\
     WHERE id >= best.id_group AND id < best.id_group + best.count\
     ORDER BY id';
  let par = [bound];
  let data = yield* query(sql, par);

  return data.rows;
};

exports.getProfitTime = function*(username, time) {
  let sql =
    'SELECT' +
    '  COALESCE(SUM(profit),0) profit' +
    '  FROM plays_view' +
    '  WHERE user_id = userIdOf($1) AND' +
    '        created >= $2';

  let par = [username, new Date(Date.now() - time)];
  let data = yield* query(sql, par);

  return data.rows[0].profit;
};

exports.getProfitGames = function*(username, games) {
  let sql =
    'WITH' +
    '  t AS (SELECT * FROM plays_view' +
    '        WHERE user_id = userIdOf($1)' +
    '        ORDER BY game_id' +
    '        DESC LIMIT $2)' +
    '  SELECT COALESCE(SUM(profit),0) profit FROM t';
  let par = [username, games];
  let data = yield* query(sql, par);

  return data.rows[0].profit;
};

exports.getSiteProfitTime = function*(time) {
  let sql =
    'SELECT' +
    '  -COALESCE(SUM(profit),0) profit' +
    '  FROM plays_view' +
    '  WHERE created >= $1';

  let par = [new Date(Date.now() - time)];
  let data = yield* query(sql, par);

  return data.rows[0].profit;
};

exports.getSiteProfitGames = function*(games) {
  let sql =
    'SELECT' +
    '  -COALESCE(SUM(profit),0) profit' +
    '  FROM plays_view' +
    '  WHERE game_id >= (SELECT MAX(id) FROM games) - $1';
  let par = [games];
  let data = yield* query(sql, par);

  return data.rows[0].profit;
};
