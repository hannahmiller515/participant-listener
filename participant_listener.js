// participant_listener.js
const VERBOSE = false;

// Twitter Setup
var twit = require('twit');  
var config = require('./config.js');
var swearjar = require('swearjar');

var tweet_count = 0;
var filtered_tweet_count = 0;
var emoji_tweet_count = 0;
var simple_emoji_tweet_count = 0;
var interval_count = 1;
var interval_seconds = 60;
var run_intervals = 60;
var run_interval = setInterval(record_interval, interval_seconds * 1000) 

var fs = require('fs');
var csv = require('fast-csv');
var csvStream = csv.createWriteStream({headers: true}),
    writableStream = fs.createWriteStream('throughput/throughput.csv');
csvStream.pipe(writableStream); 
                
// Emoji Regex Setup
const emojiRegex = require('emoji-regex');
const regex = emojiRegex();


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
        var inviteParticipant = parseTweet(tweet.text);
        if(inviteParticipant) {
            // Collect
            // tweet.user.id_str
            // tweet.user.screen_name
            // tweet.user.name
            // tweet.user.created_at
            // tweet.user.friends_count
            // tweet.user.followers_count
            // tweet.user.statuses_count
            // tweet.user.favourites_count
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
    var prevIndex = 0
    var text_frags = [];
    
    var simple_emoji_tweet = true;
    var emoji_tweet = false;
    let match;
    while (simple_emoji_tweet && (match = regex.exec(tweet_text))) {
        emoji_tweet = true;
        
        //const emoji = match[0];
        var codePoints = [...match[0]];
        if(VERBOSE) { console.log(codePoints); }
        
        // Extract preceding text fragment (between prev emoji in string and currently matched emoji)
        if(match.index > prevIndex){
            text_frags.push(tweet_text.substring(prevIndex,match.index));
        } else {
            // first char in text is an emoji
        }
        prevIndex = match.index + codePoints.length;
        
        // Ignore complex emoji sequences -- want to focus on emoji where there is cross-platform compatibility
        if(codePoints.length == 1 || (codePoints.length == 2 && codePoints[1].codePointAt(0).toString(16).toUpperCase() == 'FE0F')) {
            var code = codePoints[0].codePointAt(0).toString(16).toUpperCase();
            if(VERBOSE) { console.log(code); }
            if (code.length > 4) {
                prevIndex++;
            }
        } else {
            simple_emoji_tweet = false;
            /*
            console.log(tweet_text);
            console.log(codePoints);
            codes = [];
            for(var i = 0; i < codePoints.length; i++) {
                codes.push(codePoints[i].codePointAt(0).toString(16).toUpperCase());
            }
            console.log(codes);
            console.log();
            */
        }
    }
    if(simple_emoji_tweet && prevIndex < tweet_text.length) {
        text_frags.push(tweet_text.substring(prevIndex,tweet_text.length));
    }
    if(emoji_tweet) { 
        emoji_tweet_count++;
        if(simple_emoji_tweet) { simple_emoji_tweet_count++; }
        if(VERBOSE) {
            console.log(tweet_text);
            console.log(text_frags);
            console.log();
        } 
    }
    return emoji_tweet;
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
    clearInterval(run_interval);
    //console.log('Total tweets in stream in ' + interval_seconds + ' seconds: ' + tweet_count);
    //console.log('Tweets that suffice our filter: ' + filtered_tweet_count);
    //console.log('Filtered tweets that contain emoji: ' + emoji_tweet_count);
}