'use strict';

const _            =  require('lodash');
const co           =  require('co');
const debug        =  require('debug')('shiba');
const debugblock   =  require('debug')('shiba:blocknotify');
const debugautomute = require('debug')('shiba:automute');

const profanity    =  require('./profanity');
const Unshort      =  require('./Util/Unshort');

const Config       =  require('./Config');
//Monkey patch to socket.io to send cookies in the handshake for auth
require('socket.io-client-cookie').setCookies('id='+Config.SESSION);
const Client       =  require('./GameClient');
const WebClient    =  require('./WebClient');
const Lib          =  require('./Lib');
const Pg           =  require('./Pg');

const CmdAutomute  =  require('./Cmd/Automute');
const CmdConvert   =  require('./Cmd/Convert');
const CmdBust      =  require('./Cmd/Bust');
const CmdMedian    =  require('./Cmd/Median');
const CmdProb      =  require('./Cmd/Prob');
const CmdProfit    =  require('./Cmd/Profit');
const CmdStreak    =  require('./Cmd/Streak');

const mkCmdBlock     =  require('./Cmd/Block');
const mkAutomuteStore = require('./Store/Automute');
const mkChatStore     = require('./Store/Chat');
const mkGameStore     = require('./Store/Game');


// Command syntax
const cmdReg = /^\s*!([a-zA-z]*)\s*(.*)$/i;

function Shiba() {

  let self = this;

  co(function*(){

    // List of automute regexps
    self.automuteStore = yield* mkAutomuteStore();
    self.chatStore     = yield* mkChatStore(false);
    self.gameStore     = yield* mkGameStore(false);

    self.cmdAutomute = new CmdAutomute(self.automuteStore);
    self.cmdConvert  = new CmdConvert();
    self.cmdBlock    = yield mkCmdBlock();
    self.cmdBust     = new CmdBust();
    self.cmdMedian   = new CmdMedian();
    self.cmdProb     = new CmdProb();
    self.cmdProfit   = new CmdProfit();
    self.cmdStreak   = new CmdStreak();

    // Connect to the game server.
    self.client = new Client(Config);

    // Connect to the web server.
    self.webClient = new WebClient(Config);

    // Setup the game bindings.
    self.client.on('join', co.wrap(function*(data) {
      let games = data.table_history.sort((a,b) => a.game_id - b.game_id);
      yield* self.gameStore.mergeGames(games);
    }));
    self.client.on('game_crash', co.wrap(function*(data, gameInfo) {
      yield* self.gameStore.addGame(gameInfo);
    }));

    // Setup the chat bindings.
    self.webClient.on('join', function(data) {
      co(function*(){
        yield* self.chatStore.mergeMessages(data.history);
      }).catch(function(err) {
        console.error('Error importing history:', err);
      });
    });
    self.webClient.on('msg', function(msg) {
      co(function*(){
        yield* self.chatStore.addMessage(msg);
        if (msg.type === 'say')
          yield* self.onSay(msg);
      }).catch(err => console.error('[ERROR] onMsg:', err.stack));
    });

    self.cmdBlock.setClient(self.webClient);

    self.setupScamComment();
  }).catch(function(err) {
    // Abort immediately when an exception is thrown on startup.
    console.error(err.stack);
    throw err;
  });
}

Shiba.prototype.setupScamComment = function() {
  let self = this;
  self.client.on('game_crash', function(data) {
    let gameInfo = self.client.getGameInfo();
    console.assert(gameInfo.hasOwnProperty('verified'));

    if (gameInfo.verified !== 'ok')
      self.webClient.doSay('wow. such scam. very hash failure.', 'english');
  });
};

Shiba.prototype.checkAutomute = function*(msg) {

  // Match entire message against the regular expressions.
  let automutes = this.automuteStore.get();
  if (automutes.find(r => msg.message.match(r))) {
    this.webClient.doMute(msg.username, '3h');
    return true;
  }

  // Extract a list of URLs.
  // TODO: The regular expression could be made more intelligent.
  let urls  = msg.message.match(/https?:\/\/[^\s]+/ig) || [];
  let urls2 = msg.message.match(/(\s|^)(bit.ly|vk.cc|goo.gl)\/[^\s]+/ig) || [];
  urls2     = urls2.map(x => x.replace(/^\s*/,'http:\/\/'));
  urls      = urls.concat(urls2);

  // No URLs found.
  if (urls.length === 0) return false;

  // Unshorten extracted URLs.
  urls2 = yield* Unshort.unshorts(urls);
  urls  = urls.concat(urls2 || []);

  debugautomute('Url list: ' + JSON.stringify(urls));

  for (let url of urls) {
    debugautomute('Checking url: ' + url);

    // Run the regular expressions against the unshortened url.
    let r = automutes.find(r => url.match(r));
    if (r) {
      debugautomute('URL matched ' + r);
      this.webClient.doMute(msg.username, '6h');
      return true;
    }
  }

  return false;
};

Shiba.prototype.checkRate = function*(msg) {
  let rates = [{count: 4, seconds: 1, mute: '15m'},
               {count: 5, seconds: 5, mute: '15m'},
               {count: 8, seconds: 12, mute: '15m'}
              ];

  let self = this;
  let rate = rates.find(rate => {
    let after    = new Date(Date.now() - rate.seconds*1000);
    let messages = self.chatStore.getChatMessages(msg.username, after);
    return messages.length > rate.count;
  });

  if (rate) {
    this.webClient.doMute(msg.username, rate.mute);
    return true;
  }

  return false;
};

Shiba.prototype.onSay = function*(msg) {
  if (msg.username === this.client.username) return;

  if (yield* this.checkAutomute(msg)) return;
  if (yield* this.checkRate(msg)) return;

  // Everything checked out fine so far. Continue with the command
  // processing phase.
  let cmdMatch = msg.message.match(cmdReg);
  if (cmdMatch) yield* this.onCmd(msg, cmdMatch[1], _.trim(cmdMatch[2]));
};

Shiba.prototype.checkCmdRate = function*(msg) {
  let after    = new Date(Date.now() - 10*1000);
  let messages = this.chatStore.getChatMessages(msg.username, after);

  let count = 0;
  messages.forEach(m => { if (m.message.match(cmdReg)) ++count; });

  if (count >= 5) {
    this.webClient.doMute(msg.username, '5m');
    return true;
  } else if (count >= 4) {
    this.webClient.doSay('bites ' + msg.username);
    return true;
  }

  return false;
};

Shiba.prototype.onCmd = function*(msg, cmd, rest) {
  // Cmd rate limiter
  if (yield* this.checkCmdRate(msg)) return;

  switch(cmd.toLowerCase()) {
  case 'custom':
    yield* this.onCmdCustom(msg, rest);
    break;
  case 'lick':
  case 'lck':
  case 'lic':
  case 'lik':
  case 'lk':
    yield* this.onCmdLick(msg, rest);
    break;
  case 'seen':
  case 'sen':
  case 'sn':
  case 's':
    yield* this.onCmdSeen(msg, rest);
    break;
  case 'faq':
  case 'help':
    yield* this.onCmdHelp(msg, rest);
    break;
  case 'convert':
  case 'conver':
  case 'conv':
  case 'cv':
  case 'c':
    yield* this.onCmdConvert(msg, rest);
    break;
  case 'block':
  case 'blck':
  case 'blk':
  case 'bl':
    yield* this.cmdBlock.handle(this.webClient, msg, rest);
    break;
  case 'crash':
  case 'cras':
  case 'crsh':
  case 'cra':
  case 'cr':
    this.webClient.doSay('@' + msg.username + ' use !bust instead');
  case 'bust':
  case 'bst':
  case 'bt':
    yield* this.cmdBust.handle(this.webClient, this.client, msg, rest);
    break;
  case 'automute':
    yield* this.cmdAutomute.handle(this.webClient, msg, rest);
    break;
  case 'median':
  case 'med':
    yield* this.cmdMedian.handle(this.webClient, msg, rest);
    break;
  case 'prob':
  case 'prb':
  case 'pob':
  case 'pb':
  case 'p':
    yield* this.cmdProb.handle(this.webClient, msg, rest);
    break;
  case 'profit':
  case 'prfit':
  case 'profi':
  case 'prof':
  case 'prft':
  case 'prf':
  case 'prt':
    yield* this.cmdProfit.handle(this.webClient, msg, rest);
    break;
  case 'streak':
    yield* this.cmdStreak.handle(this.webClient, msg, rest);
    break;
  }
};

Shiba.prototype.onCmdHelp = function*(msg, rest) {
  this.webClient.doSay(
      'very explanation. much insight: ' +
      'https://github.com/moneypot/shiba/wiki/', msg.channelName);
};

Shiba.prototype.onCmdCustom = function*(msg, rest) {
  if (msg.role !== 'admin' &&
      msg.role !== 'moderator') return;

  let customReg   = /^([a-z0-9_\-]+)\s+(.*)$/i;
  let customMatch = rest.match(customReg);

  if (!customMatch) {
    this.webClient.doSay('wow. very usage failure. such retry', msg.channelName);
    this.webClient.doSay('so example, very cool: !custom Ryan very dog lover', msg.channelName);
    return;
  }

  let customUser  = customMatch[1];
  let customMsg   = customMatch[2];

  try {
    yield* Pg.putLick(customUser, customMsg, msg.username);
    this.webClient.doSay('wow. so cool. very obedient', msg.channelName);
  } catch(err) {
    console.error('[ERROR] onCmdCustom:', err.stack);
    this.webClient.doSay('wow. such database fail', msg.channelName);
  }
};

Shiba.prototype.onCmdLick = function*(msg, user) {
  user = user.toLowerCase();

  // We're cultivated and don't lick ourselves.
  if (user === this.client.username.toLowerCase()) return;

  if (profanity[user]) {
    this.webClient.doSay('so trollol. very annoying. such mute', msg.channelName);
    this.webClient.doMute(msg.username, '5m');
    return;
  }

  try {
    // Get licks stored in the DB.
    let data     = yield* Pg.getLick(user);
    let username = data.username;
    let customs  = data.licks;

    // Add standard lick to the end.
    customs.push('licks ' + username);

    // Randomly pick a lick message with lower probability of the
    // standard one.
    let r = Math.random() * (customs.length - 0.8);
    let m = customs[Math.floor(r)];
    this.webClient.doSay(m, msg.channelName);
  } catch(err) {
    if (err === 'USER_DOES_NOT_EXIST')
      this.webClient.doSay('very stranger. never seen', msg.channelName);
    else
      console.error('[ERROR] onCmdLick:', err);
  }
};

Shiba.prototype.onCmdSeen = function*(msg, user) {
  user = user.toLowerCase();

  // Make sure the username is valid
  if (Lib.isInvalidUsername(user)) {
    this.webClient.doSay('such name. very invalid', msg.channelName);
    return;
  }

  // In case a user asks for himself.
  if (user === msg.username.toLowerCase()) {
    this.webClient.doSay('go find a mirror @' + msg.username, msg.channelName);
    return;
  }

  // Special treatment of block.
  if (user === 'block') {
    yield* this.cmdBlock.handle(this.webClient, msg, user);
    return;
  }

  // Special treatment of rape.
  if (user === 'rape') {
    yield* this.cmdBust.handle(this.webClient, this.client, msg, '< 1.05');
    return;
  }

  // Somebody asks us when we've seen ourselves.
  if (user === this.client.username.toLowerCase()) {
    this.webClient.doSay('strange loops. much confusion.', msg.channelName);
    return;
  }

  if (profanity[user]) {
    this.webClient.doSay('so trollol. very annoying. such mute', msg.channelName);
    this.webClient.doMute(msg.username, '5m');
    return;
  }

  let message;
  try {
    message = yield* Pg.getLastSeen(user);
  } catch(err) {
    if (err === 'USER_DOES_NOT_EXIST')
      this.webClient.doSay('very stranger. never seen', msg.channelName);
    else {
      console.error('[ERROR] onCmdSeen:', err.stack);
      this.webClient.doSay('wow. such database fail', msg.channelName);
    }
    return;
  }

  if (!message.time) {
    // User exists but hasn't said a word.
    this.webClient.doSay('very silent. never spoken', msg.channelName);
    return;
  }

  let diff = Date.now() - message.time;
  let line;
  if (diff < 1000) {
    line = 'Seen ' + message.username + ' just now.';
  } else {
    line = 'Seen ' + message.username + ' ';
    line += Lib.formatTimeDiff(diff);
    line += ' ago.';
  }
  this.webClient.doSay(line, msg.channelName);
};

Shiba.prototype.onCmdConvert = function*(msg, conv) {
  yield* this.cmdConvert.handle(this.webClient, msg, conv);
};

let shiba = new Shiba();
