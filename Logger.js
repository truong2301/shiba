'use strict';

const co     =  require('co');
const fs     =  require('fs');

const Client =  require('./GameClient');
const WebClient =  require('./WebClient');
const Lib    =  require('./Lib');
const Pg     =  require('./Pg');
const Config =  require('./Config');

const mkChatStore = require('./Store/Chat');
const mkGameStore = require('./Store/Game');

function ensureDirSync(dir) {
  try { fs.mkdirSync(dir); }
  catch(e) { if (e.code !== 'EEXIST') throw e; }
}
ensureDirSync('gamelogs');
ensureDirSync('gamelogs/unfinished');
ensureDirSync('chatlogs');

co(function*(){

  let chatStore = yield* mkChatStore(true);
  let gameStore = yield* mkGameStore(true);

  // Connect to the site
  let client = new Client(Config);
  let webClient = new WebClient(Config);

  client.on('join', co.wrap(function*(data) {
    let games = data.table_history.sort((a,b) => a.game_id - b.game_id);
    yield* gameStore.mergeGames(games);
  }));
  client.on('game_crash', co.wrap(function*(data, gameInfo) {
    yield* gameStore.addGame(gameInfo);
  }));

  webClient.on('join', function(data) {
    co(function*(){
      yield* chatStore.mergeMessages(data.history);
    }).catch(function(err) {
      console.error('Error importing history:', err);
    });
  });
  webClient.on('msg', co.wrap(chatStore.addMessage.bind(chatStore)));


  // Setup chatlog file writer
  let chatDate    = null;
  let chatStream  = null;

  chatStore.on('msg', function(msg) {
    // Write to the chatlog file. We create a file for each date.
    let now = new Date(Date.now());

    if (!chatDate || now.getUTCDay() !== chatDate.getUTCDay()) {
      // End the old write stream for the previous date.
      if (chatStream) chatStream.end();

      // Create new write stream for the current date.
      let chatFile =
        "chatlogs/" + now.getUTCFullYear() +
        ('0'+(now.getUTCMonth()+1)).slice(-2) +
        ('0'+now.getUTCDate()).slice(-2) + ".log";
      chatDate   = now;
      chatStream = fs.createWriteStream(chatFile, {flags:'a'});
    }
    chatStream.write(JSON.stringify(msg) + '\n');
  });
}).catch(function(err) {
  // Abort immediately when an exception is thrown on startup.
  console.error(err.stack);
  throw err;
});
