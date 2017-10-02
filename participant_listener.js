// participant_listener.js
const VERBOSE = true;
var config = require('./local_config.js');

// Database Setup
const mysql = require('mysql');
const connection = mysql.createConnection(config);
connection.connect((err) => {
    if (err) {
        console.log(err);
        console.log('Error connecting to Db');
        return;
    }
    console.log('Connected to Db');
});

// participant, tweet, tweet fragment, survey insert queries
const participant_query = 'INSERT INTO participants SET ?';
const tweet_query = 'INSERT INTO tweets SET ?';
const tweet_frag_query = 'INSERT INTO tweet_fragments SET ?';
const survey_query = 'INSERT INTO surveys SET ?';
const emoji_query = 'SELECT codepoint_string,emoji_id FROM emoji ORDER BY num_renderings desc,num_platforms_support desc;';

// load the emoji dictionary (for emoji id lookup by codepoint string): codepoint_string => emoji_id
let emoji_dict = {};
connection.query(emoji_query, function (error, results, fields) {
    results.forEach( (row) => {
        emoji_dict[row.codepoint_string] = row.emoji_id;
    });
    console.log('Emoji dictionary loaded');
});

// Emoji Regex Setup
const emojiRegex = require('emoji-regex');
const regex = emojiRegex();

// Throughput Output Setup
var fs = require('fs');
var csv = require('fast-csv');
var csvStream = csv.createWriteStream({headers: true});
var writableStream = fs.createWriteStream('throughput/throughput.csv');
csvStream.pipe(writableStream);

var tweet_count = 0;
var filtered_tweet_count = 0;
var emoji_tweet_count = 0;
var simple_emoji_tweet_count = 0;
var interval_count = 1;
const interval_seconds = 20;
const run_intervals = 1;
const run_interval = setInterval(record_interval, interval_seconds * 1000);

// Twitter Setup
var twit = require('twit');
var swearjar = require('swearjar');

var TweetStacks = [[],[],[],[]];
var sourceDict = {apple:0,android:1,windows:2,twitter:3};
var TweetCounts = [{source_id:0,source:'apple',tweet_count:0},      // TODO tweet counts need to transcend "runs" of this program
                   {source_id:1,source:'android',tweet_count:0},    // TODO (store counts in database / keep track over time)
                   {source_id:2,source:'windows',tweet_count:0},
                   {source_id:3,source:'twitter',tweet_count:0}];
// Sort tweet counts from lowest to highest (so the lowest will be next to have a tweet sent)
function sort_tweet_counts(a,b) {
    return a.tweet_count - b.tweet_count;
}

const tweet_interval_seconds = 5;
const send_tweet_interval = setInterval(send_tweet, tweet_interval_seconds * 1000);
const tweet_templates = [' We’re Univ of MN researchers studying emoji usage. Pls take our ~5min survey to help us learn more. ',
                         ' We’re Univ of MN researchers studying emoji and we noticed you just tweeted one. Pls help us learn more via a short survey: '];
// TODO Tweet templates (check account tweet capacity)

// TWITTER STREAM AND HANDLERS
var Twitter = new twit(config);
var stream = Twitter.stream('statuses/sample');

// Handler for a tweet coming into the stream 
stream.on('tweet', function (tweet) {
    tweet_count++;
        
    // Filter tweets:
    if(tweet.lang=='en' &&                          // english
       tweet.retweeted == false &&                  // not retweeted
       tweet.text.substring(0,2) != "RT" &&         // not retweeted
       tweet.entities.urls.length == 0 &&           // no urls
       tweet.entities.media == null &&              // no media
       tweet.entities.user_mentions.length == 0 &&  // no user mentions (only doing broadcast)
       tweet.in_reply_to_status_id == null &&       // not in reply to a status
       tweet.in_reply_to_user_id == null &&         // not in reply to a user
       tweet.source.startsWith("<a href=\"http://twitter.com") &&  // control source
       profanity_check(tweet)                       // not profane
       ) {
        
        filtered_tweet_count++;
        [num_emoji,tweet_fragments] = parseTweet(tweet.text);

        if(num_emoji > 0) {
            var source = undefined;
            if(tweet.source.lastIndexOf('Twitter for iPhone')!=-1 || tweet.source.lastIndexOf('Twitter for iPad')!=-1 || tweet.source.lastIndexOf('Twitter for Mac')!=-1) {
                source = 'apple';
            } else if(tweet.source.lastIndexOf('Twitter for Android')!=-1) {
                source = 'android';
            } else if(tweet.source.lastIndexOf('Twitter for Windows')!=-1) {
                source = 'windows';
            } else if(tweet.source.lastIndexOf('Twitter Web Client')!=-1) {
                source = 'twitter';
            }

            if(source) {
                curStack = TweetStacks[sourceDict[source]];
                curStack.push([tweet,num_emoji,tweet_fragments]);
                if(curStack.length > 10) {
                    curStack.shift();
                }
                if (VERBOSE) { console.log('pushing tweet onto stack for ' + source + ' (' + curStack.length + ' on stack)'); }
            }
        }
    }
});

// Handler for a "limitation message" coming into the stream
stream.on('limit', function (limitMessage) {
  //...
  console.log(limitMessage);
  stop_program();
});

stream.on('error', function(err) {
    console.log(err.message);
    stream.stop();
    stream.start();
});

// Return true if passes check, false if not
function profanity_check(tweet) {
    return !(swearjar.profane(tweet.text) ||                // check tweet
             swearjar.profane(tweet.user.screen_name) ||    // check user twitter handle
             swearjar.profane(tweet.user.name) ||           // check user display name
             swearjar.profane(tweet.user.description));     // check user description
}

// Function to parse tweet for emoji
function parseTweet(tweet_text) {
    var prevIndex = 0;
    var num_emoji = 0;
    var tweet_fragments = [];
    // tweet fragment: [isText, value] | value = codepoint string or text fragment

    let match;
    while (match = regex.exec(tweet_text)) {
        num_emoji++;
        
        //const emoji = match[0];
        var codePoints = [...match[0]];
        
        // Extract preceding text fragment (between prev emoji in string and currently matched emoji)
        if(match.index > prevIndex){
            var text_frag = tweet_text.substring(prevIndex,match.index);
            tweet_fragments.push({isText:true,value:text_frag});
        }
        prevIndex = match.index + codePoints.length;

        codes = [''];
        for(var i = 0; i < codePoints.length; i++) {
            var code = codePoints[i].codePointAt(0).toString(16).toUpperCase();
            codes.push(code);
            if (code.length > 4) {
                prevIndex++;
            }
        }
        tweet_fragments.push({isText:false,value:codes.join('U+')});
    }

    if(prevIndex < tweet_text.length) {
        var text_frag = tweet_text.substring(prevIndex,tweet_text.length);
        tweet_fragments.push({isText:true,value:text_frag});
    }
    if(num_emoji>0) {
        emoji_tweet_count++;
        if(VERBOSE) {
            console.log(tweet_text);
            console.log(tweet_fragments);
            console.log();
        } 
    }
    return [num_emoji,tweet_fragments];
}

function send_tweet() {
    if (VERBOSE) { console.log(); console.log(); console.log('SENDING TWEET'); }
    // TODO random sleep

    let tweet_counts_index = 0;
    while (tweet_counts_index< 4 && TweetStacks[TweetCounts[tweet_counts_index].source_id].length == 0) {
        // increment the stack
        tweet_counts_index++;
    }
    if(tweet_counts_index == 4) {
        console.log('no tweets queued');
        return;
    }

    if (VERBOSE) { console.log('from ' + TweetCounts[tweet_counts_index].source + ' stack'); }
    var curStackIndex = TweetCounts[tweet_counts_index].source_id;
    [tweet,num_emoji,tweet_frags] = TweetStacks[curStackIndex].pop();

    var participant_data = {twitter_id_str:tweet.user.id_str,
                            twitter_handle:tweet.user.screen_name,
                            display_name:tweet.user.name,
                            account_created_at:tweet.user.created_at,
                            friends_count:tweet.user.friends_count,
                            followers_count:tweet.user.followers_count,
                            statuses_count:tweet.user.statuses_count,
                            favorites_count:tweet.user.favourites_count};

    connection.query(participant_query, participant_data, function (error, results, fields) {
        if (error) {
            console.log('ERROR inserting participant: ' + error);
            send_tweet();
            return;
        }
        var participant_id = results.insertId;
        if (VERBOSE) { console.log('inserted participant at id ' + participant_id); }

        var tweet_data = {text:tweet.text,
                          num_emoji:num_emoji,
                          source_id:curStackIndex+1,
                          tweet_created_at:tweet.created_at};
        connection.query(tweet_query, tweet_data, function (error, results, fields) {
            if (error) throw error;
            var tweet_id = results.insertId;
            if (VERBOSE) { console.log('inserted tweet at id ' + tweet_id); }

            var sequence = 1;
            tweet_frags.forEach( (tweet_frag) => {
                var text = tweet_frag.isText ? tweet_frag.value : undefined;
                var emoji_id = !tweet_frag.isText ? emoji_dict[tweet_frag.value] : undefined;
                var tweet_frag_data = {tweet_id:tweet_id,
                                       is_text:tweet_frag.isText,
                                       text:text,
                                       emoji_id:emoji_id,
                                       sequence_index:sequence};
                connection.query(tweet_frag_query, tweet_frag_data, function (error, results, fields) {
                    if (error) throw error;
                    if (VERBOSE) { console.log('inserted tweet fragment'); }
                });
                sequence++;
            });

            var survey_data = {participant_twitter_handle:tweet.user.screen_name,
                               participant_id:participant_id,
                               tweet_id:tweet_id};
            connection.query(survey_query, survey_data, function (error, results, fields) {
                if (error) throw error;
                var survey_id = results.insertId;
                if (VERBOSE) { console.log('inserted survey at id ' + survey_id); }

                // TODO create short link with survey id
                var link = 'http://emojistudy.umn.edu/survey/id/' + survey_id;

                var tweet_to_send = '@' + tweet.user.screen_name + tweet_templates[Math.floor(Math.random()*tweet_templates.length)] + link;
                console.log('TWEET:');
                console.log(tweet_to_send);

                // TODO send (reply)tweet
                if (VERBOSE) { console.log('sending tweet...'); console.log(); }

                TweetCounts[tweet_counts_index].tweet_count++;
                TweetCounts.sort(sort_tweet_counts);
                if (VERBOSE) { console.log(TweetCounts); console.log(); console.log(); }
            });
        });
    });
}

function record_interval() {
    csvStream.write({
        interval: interval_count,
        total_tweets: tweet_count,
        filtered_tweets: filtered_tweet_count,
        emoji_tweets: emoji_tweet_count,
        simple_emoji_tweets: simple_emoji_tweet_count
    });
    interval_count++;
    if(interval_count <= run_intervals) {
        tweet_count = 0;
        filtered_tweet_count = 0;
        emoji_tweet_count = 0;
        simple_emoji_tweet_count = 0;
    } else {
        stop_program();
    }
}

function stop_program() {
    stream.stop();
    csvStream.end();
    connection.destroy();
    clearInterval(send_tweet_interval);
    clearInterval(run_interval);
    //console.log('Total tweets in stream in ' + interval_seconds + ' seconds: ' + tweet_count);
    //console.log('Tweets that suffice our filter: ' + filtered_tweet_count);
    //console.log('Filtered tweets that contain emoji: ' + emoji_tweet_count);
}