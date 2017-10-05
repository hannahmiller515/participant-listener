// participant_listener.js
var config = require('./local_config.js');
const VERBOSE = config.verbose;
const send_tweets_automatically = config.send_tweets_automatically;

// Database Setup
const mysql = require('mysql');
const connection = mysql.createConnection(config);
connection.connect((err) => {
    if (err) {
        console.log(err);
        console.log('Error connecting to Db');
        return;
    }
});

// participant, tweet, tweet fragment, survey insert queries
const participant_query = 'INSERT INTO participants SET ?';
const tweet_query = 'INSERT INTO tweets SET ?';
const tweet_frag_query = 'INSERT INTO tweet_fragments SET ?';
const survey_query = 'INSERT INTO surveys SET ?';
const tweet_not_sent_query = 'UPDATE surveys SET ? WHERE survey_id=?';

// emoji and tweet source count queries
const emoji_query = 'SELECT codepoint_string,emoji_id FROM emoji ORDER BY num_renderings desc,num_platforms_support desc;';
const tweet_count_query = 'SELECT sources.source_id,source_name as source,sum(tweet_sent) as tweet_count from sources LEFT JOIN tweets ON tweets.source_id=sources.source_id LEFT JOIN surveys on surveys.tweet_id=tweets.tweet_id GROUP BY source_id;';

// load the emoji dictionary (for emoji id lookup by codepoint string): codepoint_string => emoji_id
let emoji_dict = {};
connection.query(emoji_query, function (error, results, fields) {
    results.forEach( (row) => {
        emoji_dict[row.codepoint_string] = row.emoji_id;
    });
});

// Emoji Regex Setup
const emojiRegex = require('emoji-regex');
const regex = emojiRegex();

// Z API Setup
var jwt = require('jsonwebtoken');
var Client = require('node-rest-client').Client;
var client = new Client();

// Throughput Output Setup
var fs = require('fs');
var csv = require('fast-csv');
var csvStream = csv.createWriteStream({headers: config.throughput_headers});
var writableStream = fs.createWriteStream(config.throughput_file, config.throughput_flags);
csvStream.pipe(writableStream);

var tweet_count = 0;
var filtered_tweet_count = 0;
var emoji_tweet_count = 0;
var apple_count = 0;
var android_count = 0;
var windows_count = 0;
var twitter_count = 0;
var interval_count = 1;
const interval_seconds = 60;
const run_intervals = undefined;
const run_interval = setInterval(record_interval, interval_seconds * 1000);

// Twitter Setup
// Create tweet stack for each source, indexed by the source id (in the database) (zero index placeholder)
var TweetStacks = [[],[],[],[],[]];
var sourceDict = {android:1,apple:2,windows:3,twitter:4};
// load the tweet counts for each source
var TweetCounts = [];
connection.query(tweet_count_query, function (error, results, fields) {
    results.forEach( (row) => {
        TweetCounts.push({source_id:row.source_id,source:row.source,tweet_count:row.tweet_count,tweet_count_at_start:row.tweet_count});
    });
    TweetCounts.sort(sort_tweet_counts);
});

// Sort tweet counts from lowest to highest (so the lowest will be next to have a tweet sent)
function sort_tweet_counts(a,b) {
    return a.tweet_count - b.tweet_count;
}

// get random number of seconds between 45 and 80
var random_tweet_interval = function() {
    var min_seconds = 45;
    var max_seconds = 80;
    var interval = Math.ceil(Math.random()*(max_seconds-min_seconds)+min_seconds);
    if (VERBOSE) { console.log('timeout set: next tweet will be sent in ' + interval + ' seconds'); }
    return interval*1000;
}
var send_tweet_timeout = setTimeout(send_tweet, random_tweet_interval());
var send_account_tweet_timeout = setTimeout(send_account_tweet, (45*60));

// TWITTER STREAM AND HANDLERS
var twit = require('twit');
var Twitter = new twit(config);
var stream = Twitter.stream('statuses/sample');
var swearjar = require('swearjar');
var tweet_templates = require('./tweet_templates.js').tweet_templates;
var account_tweet_templates = require('./account_tweets.js').tweets;
var cur_account_tweet = 0;

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
                apple_count++;
            } else if(tweet.source.lastIndexOf('Twitter for Android')!=-1) {
                source = 'android';
                android_count++;
            } else if(tweet.source.lastIndexOf('Twitter for Windows')!=-1) {
                source = 'windows';
                windows_count++;
            } else if(tweet.source.lastIndexOf('Twitter Web Client')!=-1) {
                source = 'twitter';
                twitter_count++;
            }

            if(source) {
                curStack = TweetStacks[sourceDict[source]];
                curStack.push([tweet,num_emoji,tweet_fragments]);
                if(curStack.length > 10) {
                    curStack.shift();
                }
                if (VERBOSE) { console.log('pushing tweet onto stack for ' + source + ' (' + curStack.length + ' on stack)'); console.log(); }
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
    // tweet fragment: {isText:, value:} | value = codepoint string or text fragment

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

        if(codes.length==3 && codes[2]=='FE0F'){ codes.pop(); }
        tweet_fragments.push({isText:false,value:codes.join('U+')});
    }

    if(prevIndex < tweet_text.length) {
        var text_frag = tweet_text.substring(prevIndex,tweet_text.length);
        tweet_fragments.push({isText:true,value:text_frag});
    }
    if(num_emoji>0) {
        emoji_tweet_count++;
        if(VERBOSE) {
            console.log();
            console.log('EMOJI TWEET FOUND');
            console.log(tweet_text);
            console.log(tweet_fragments);
        } 
    }
    return [num_emoji,tweet_fragments];
}

function send_tweet() {
    console.log();
    if (VERBOSE) { console.log('SENDING TWEET'); }
    send_tweet_timeout = setTimeout(send_tweet, random_tweet_interval());

    let tweet_counts_index = 0;
    while (tweet_counts_index< 4 && TweetStacks[TweetCounts[tweet_counts_index].source_id].length == 0) {
        // increment the stack
        tweet_counts_index++;
    }
    if(tweet_counts_index == 4) {
        console.log('no tweets queued');
        console.log();
        return;
    }

    if (VERBOSE) { console.log('current tweet from ' + TweetCounts[tweet_counts_index].source + ' stack'); }
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
            clearTimeout(send_tweet_timeout);
            send_tweet();
            return;
        }
        var participant_id = results.insertId;
        if (VERBOSE) { console.log('inserted participant at id ' + participant_id); }

        var tweet_data = {text:tweet.text,
                          num_emoji:num_emoji,
                          source_id:curStackIndex,
                          tweet_created_at:tweet.created_at};
        connection.query(tweet_query, tweet_data, function (error, results, fields) {
            if (error) throw error;
            var tweet_id = results.insertId;
            console.log(tweet.text);
            console.log('inserted tweet at id ' + tweet_id);

            var sequence = 1;
            var emoji_not_found = false;
            tweet_frags.forEach( (tweet_frag) => {
                if(!emoji_not_found) {
                    var emoji_id = !tweet_frag.isText ? emoji_dict[tweet_frag.value] : undefined;
                    if (!tweet_frag.isText && emoji_id == undefined) {
                        console.log('EMOJI NOT FOUND: ' + tweet_frag.value);
                        emoji_not_found = true;
                    }

                    var text = tweet_frag.isText ? tweet_frag.value : undefined;
                    var tweet_frag_data = {
                        tweet_id: tweet_id,
                        is_text: tweet_frag.isText,
                        text: text,
                        emoji_id: emoji_id,
                        sequence_index: sequence
                    };
                    connection.query(tweet_frag_query, tweet_frag_data, function (error, results, fields) {
                        if (error) throw error;
                        if (VERBOSE) {
                            console.log('inserted tweet fragment');
                        }
                    });
                    sequence++;
                }
            });
            if(emoji_not_found) {
                console.log('Skipping this tweet');
                clearTimeout(send_tweet_timeout);
                send_tweet();
                return;
            }

            var invite_template = Math.floor(Math.random()*tweet_templates.length);
            var survey_data = {participant_twitter_handle:tweet.user.screen_name,
                               participant_id:participant_id,
                               tweet_id:tweet_id,
                               invite_template:invite_template,
                               tweet_sent:send_tweets_automatically};
            connection.query(survey_query, survey_data, function (error, results, fields) {
                if (error) throw error;
                var survey_id = results.insertId;
                console.log('inserted survey at id ' + survey_id);

                var link = config.study_link + survey_id;
                var payload = {
                    urls: [
                        { url: link,
                          collection: config.z_api_collection }
                    ]
                }
                var token = jwt.sign(payload, config.z_api_secret_key);
                var args = {
                    headers: { "Content-Type": "application/json",
                               "Authorization": config.z_api_access_id+':'+token }
                };

                client.post("https://z.umn.edu/api/v1/urls", args, function (data, response) {
                    if (data[0].result.status == 'success') {
                        link = data[0].result.message; //
                        if (VERBOSE) { console.log('short link created: ' + link); }

                        var handle = tweet.user.screen_name
                        var tweet_to_send = tweet_templates[invite_template].replace('<handle>',handle) + ' ' + link;

                        console.log('TWEET:');
                        console.log(tweet_to_send);
                        console.log();

                        var parameters = {
                            status: tweet_to_send,
                            in_reply_to_status_id: tweet.id_str,
                            //in_reply_to_status_id: '877623268257214464' // <- one of my tweets (@hannahjean515)
                        }

                        if (send_tweets_automatically) {
                            if (VERBOSE) { console.log('sending tweet...'); }
                            Twitter.post('statuses/update', parameters, function(err, data, response) {
                                if(err) {
                                    console.log('Error posting tweet:');
                                    console.log(err);
                                    console.log();

                                    var tweet_not_sent_data = { tweet_sent: false }
                                    connection.query(tweet_not_sent_query, [tweet_not_sent_data,survey_id], function (error, results, fields) {
                                        if (error) throw error;
                                        clearTimeout(send_tweet_timeout);
                                        send_tweet();
                                    });
                                } else {
                                    TweetCounts[tweet_counts_index].tweet_count++;
                                    TweetCounts.sort(sort_tweet_counts);
                                    if (VERBOSE) { console.log('sent!'); console.log(); console.log(TweetCounts); console.log(); }
                                }
                            });
                        } else {
                            TweetCounts[tweet_counts_index].tweet_count++;
                            TweetCounts.sort(sort_tweet_counts);
                            if (VERBOSE) { console.log(); console.log(TweetCounts); console.log(); }
                        }
                    } else {
                        console.log('error creating z short link:');
                        console.log(data[0].result.message);
                        var tweet_not_sent_data = { tweet_sent: false }
                        connection.query(tweet_not_sent_query, [tweet_not_sent_data,survey_id], function (error, results, fields) {
                            if (error) throw error;
                            clearTimeout(send_tweet_timeout);
                            send_tweet();
                        });
                    }
                });
            });
        });
    });
}

function send_account_tweet() {
    send_account_tweet_timeout = setTimeout(send_account_tweet, (45*60));
    var parameters = {
        status:account_tweet_templates[cur_account_tweet]
    }
    Twitter.post('statuses/update', parameters, function(err, data, response) {
        if(err) {
            console.log('Error posting account tweet:');
            console.log(err);
            console.log();
        } else {
            console.log('sent account tweet:');
            console.log(account_tweet_templates[cur_account_tweet])
            console.log();
            cur_account_tweet = (cur_account_tweet+1) % account_tweet_templates.length;
        }
    });
}

function record_interval() {
    if(VERBOSE) { console.log('recording throughput'); }
    if (interval_count==1) {
        if(!config.throughput_headers) { csvStream.write({
            interval: '',
            total_tweets: '',
            filtered_tweets: '',
            emoji_tweets: '',
            apple_tweets: '',
            android_tweets: '',
            windows_tweets: '',
            twitter_tweets: '',
            empty:''}); }
    }
    csvStream.write({
        interval: interval_count,
        total_tweets: tweet_count,
        filtered_tweets: filtered_tweet_count,
        emoji_tweets: emoji_tweet_count,
        apple_tweets: apple_count,
        android_tweets: android_count,
        windows_tweets: windows_count,
        twitter_tweets: twitter_count,
        empty:''
    });

    interval_count++;
    tweet_count = 0;
    filtered_tweet_count = 0;
    emoji_tweet_count = 0;
    apple_count = 0;
    android_count = 0;
    windows_count = 0;
    twitter_count = 0;

    if(run_intervals != undefined && interval_count == run_intervals) {
        stop_program();
    }
}

function stop_program() {
    stream.stop();
    csvStream.end();
    connection.destroy();
    clearTimeout(send_tweet_timeout);
    clearTimeout(send_account_tweet_timeout);
    clearInterval(run_interval);
    //console.log('Total tweets in stream in ' + interval_seconds + ' seconds: ' + tweet_count);
    //console.log('Tweets that suffice our filter: ' + filtered_tweet_count);
    //console.log('Filtered tweets that contain emoji: ' + emoji_tweet_count);
}