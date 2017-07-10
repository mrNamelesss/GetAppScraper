var DATABASE_HOST = 'localhost';
var DATABASE_PORT = 28015;
var DATABASE_NAME = 'test';
var DATABASE_TABLE_NAME = 'GetAppScraper';
var NUMBER_OF_THREADS = -1000;
var SLEEP_MS = 30000;

////////////////////////////////////////////////////////////////////////////////////////////
// NUMBER_OF_THREADS = 10 => getapp.com gives 429-error after ~1500 requests
// NUMBER_OF_THREADS = 2  => all right, but 14302 links will be browsed within ~88 minutes
// NUMBER_OF_THREADS = 3  => all right too
// SLEEP_MS = 30000       => a thread must sleep 30000 after fail
////////////////////////////////////////////////////////////////////////////////////////////
// 07.07.17 UPDATE:
// NUMBER_OF_THREADS = -1000 => wait 1 sec between requests,
//                              but even this can cause 429 error sometimes
////////////////////////////////////////////////////////////////////////////////////////////

var tress =          require('tress');		// async.queue
var needle =         require('needle');		// request
var cheerio =        require('cheerio');		// $
var resolve =        require('url').resolve; // some util
var fs =             require('fs');			// file system
var colors =         require('colors');		// colorize the console
var r =              require('rethinkdb');	// RethinkDB driver
var processOnDeath = require('death');		// when terminating by CTRL+C


var tasksFile =  './tasks.json';
var URL =        'https://www.getapp.com/browse';
var countPages = 0;
var countApps =  0;
var inserted =   0;
var replaced =   0;
var unchanged =  0;
var errors =     0;
var connection;


console.time("App worked: ".grey); // label to measure working time



// connect to the DB, then run the main function
r.connect({
  host: DATABASE_HOST,
  port: DATABASE_PORT
}, function (err, conn) {
  if (err) throw err;
  connection = conn;
  console.log(`Connected to DB on ${DATABASE_HOST}:${DATABASE_PORT} successfully!`.green);

  main();

});



function main() {

  // request options: follow redirects two times
  var opts = {
    follow_max: 2
    //,proxy: "168.102.134.47:8080"
  };

  // setup queue worker function, that handles tasks (i.e. pages)
  var q = tress(function (page, cb) {
    var url = page.url;
    var appID = page.appID;

    needle.get(url, opts, function (err, res) {

      // if there was an error while GETting url, then wait for .. sec and retry
      if (err || res.statusCode !== 200) {
        console.error(`Error: ${(err || res.statusCode)}. Paused for ${SLEEP_MS / 1000} ms. (${url.split('/').slice(-2)[0]})`.red);
        return cb(true); // place url in the beginning of the queue
      }


      // parse DOM
      var $ = cheerio.load(res.body);

      countPages++;


      if ($('div.serp-listings').length == 1) {
        // list of apps page

        $('li.row.listing_entry').each(function () {

          if ($(this).find('a.btn-block.btn-primary').text().trim().toLowerCase() == 'visit website') {
            // 'VISIT PAGE'

            var href = $(this).find('a.btn-block.btn.btn-raised.btn-primary.evnt.btn-md').attr('href');
            var urlIndex = href.indexOf('&url=http');
            if (urlIndex == -1) {
              var appSite = href.split('/')[2].split('?')[0];
              appSite += '.com';
            } else {
              var twoSlashesIndex = href.indexOf('%2F%2F', urlIndex);
              var slashOrAmpIndex = href.indexOf('%2F', twoSlashesIndex + 6);
              if (slashOrAmpIndex == -1) slashOrAmpIndex = href.indexOf('&', twoSlashesIndex + 6);
              var appSite = href.slice(twoSlashesIndex + 6, slashOrAmpIndex);
            }
          }

          if ($(this).find('a.btn-block.btn-primary').text().trim().toLowerCase() == 'learn more') {
            // 'LEARN MORE'

            var href = $(this).find('a.btn-block.btn.btn-primary.evnt.btn-md').attr('href');
            var appSite = href.split('/').slice(-2)[0];
            appSite += '.com';
          }


          if (!appSite) appSite = "ERROR " + new Date();

          // put task in the head of the queue
          q.unshift({
            url: resolve(URL, $(this).find('a.evnt').attr('href')),
            appID: appSite
          });
        });

        // 'next' button
        if ($('ul.pagination a:contains("Next")').length) {
          q.push({
            url: resolve(URL, $('ul.pagination a:contains("Next")').attr('href')),
            appID: null
          });
        }

      }





      if ($('button.association-type-save').length == 1) {
        // app page


        if (appID.length > 127) {
          console.log("ID is too long, will be truncated to 127 characters! ID: ".red + appID.grey)
          appID = appID.slice(0, 123) + '.com';
        }

        var categories;
        var isRankingInCategories = $('h3:contains("Specifications") ~ div.row div.col-lg-3:contains("Ranking in Categories") + div.col-lg-9').length;
        var isCategories = $('h3:contains("Specifications") ~ div.row div.col-lg-3:contains("Categories") + div.col-lg-9').length;

        if (isRankingInCategories) {
          // Ranking in Categories
          categories = [];
          $('h3:contains("Specifications") ~ div.row div.col-lg-3:contains("Ranking in Categories") + div.col-lg-9 strong').each(function () {
            categories.push($(this).text().trim());
          });

        } else if (isCategories) {
          // Categories
          var text = $('h3:contains("Specifications") ~ div.row div.col-lg-3:contains("Categories") + div.col-lg-9').text();
          categories = text.split('·').map(item => item.trim()).filter(item => item.length ? true : false);
        }

        if (!categories) categories = ['ERROR'];

        // save the app
        // in case of the same ID => rewrite
        r.db(DATABASE_NAME).table(DATABASE_TABLE_NAME).insert({
          id: appID,
          function_tags: categories,
          //name: $('h2.cut').eq(0).text().trim(),
          //url: url
          //description: $('p.lead.text-muted.cut.ellipsis').text().trim()
        }, {
            conflict: 'update'
          }).run(connection, function (err, stats) {
            if (err) throw err;
            if (stats.first_error) throw stats.first_error;

            countApps++;
            inserted += stats.inserted;
            replaced += stats.replaced;
            unchanged += stats.unchanged;
            errors += stats.errors;

          });

      }


      // return
      return cb();

    });
  }, NUMBER_OF_THREADS); // run in %second_param% parallel threads (or -%second_param% ms delay)





  // all jobs done
  q.drain = finish;

  function finish() {
    console.log("Done!");

    setTimeout(function () {
      console.log(`Pages: ${countPages}, Apps: ${countApps}. Inserted: ${inserted}, Replaced: ${replaced}, Unchanged: ${unchanged}, Errors: ${errors}`.green);

      if (fs.existsSync(tasksFile)) {
        fs.unlinkSync(tasksFile)
      }

      // detach from the DB
      connection.close(function (err) {

        console.timeEnd("App worked: ".grey);
        if (err) throw err;
        process.exit();

      });

    }, 3000);


  }



  // retry request if there was an error
  q.retry = function () {
    q.pause();
    //console.log('Paused on:', this);
    setTimeout(function () {
      q.resume();
      console.log('Resumed.'.green);
    }, SLEEP_MS);
  }



  // feed up a queue with links
  function setupScraper() {

    if (fs.existsSync(tasksFile)) {
      // if there are tasks left
      var tasks = JSON.parse(fs.readFileSync(tasksFile, 'utf-8'));
      tasks = tasks.tasks;

      if (tasks.waiting.length == 0) {
        console.log('All tasks done. I\'m deleting tasks.json to start scraping from the beginning.'.green);
        finish();
      }

      q.load(tasks);
      console.log(`Continue scraping... (${tasks.waiting.length} tasks loaded)`.grey);

    } else {
      // start scraping from the beginning

      var linksCount = 0;

      needle.get(URL, opts, function (err, res) {
        if (err) throw err;

        var $ = cheerio.load(res.body);

        var totalPagesCountArr = [];
        $('div.masonry div.block').each(function () {
          $(this).find('ul li span.text-muted').each(function () {
            totalPagesCountArr.push($(this).text().trim());
          });
        });

        var totalPagesCount = totalPagesCountArr
          .map(item => item.slice(1))
          .map(item => parseInt(item))
          .reduce((prev, curr) => prev + curr);

        $('div.masonry div.block').each(function () {
          $(this).find('ul a').each(function () {
            q.push({
              url: resolve(URL, $(this).attr('href')),
              appID: null
            });
            linksCount++;
          });
        });

        console.log("Categories: ".grey + linksCount);
        console.log("Apps: ".grey + totalPagesCount);
        console.log("Collecting links...".grey);

        //console.log("LENGTH = " + q.length());
      });

    }

  }

  setupScraper();



  // repeating logging
  setInterval(function () {
    console.log(`Pages: ${countPages}, Apps: ${countApps}. Inserted: ${inserted}, Replaced: ${replaced}, Unchanged: ${unchanged}`.cyan);
  }, 5000);

  // logging how many objects in DATABASE_TABLE_NAME
  setInterval(function () {
    r.db(DATABASE_NAME).table(DATABASE_TABLE_NAME).count().run(connection, function (err, howMany) {
      if (err) throw err;
      console.log(`Objects in DB: ${howMany}`.cyan);
    });
  }, 60000);


  // when terminating the process by CTRL+C
  processOnDeath(function (signal, err) {
    console.log(`Pages: ${countPages}, Apps: ${countApps}. Inserted: ${inserted}, Replaced: ${replaced}, Unchanged: ${unchanged}, Errors: ${errors}`.red);

    q.save(function (tasks) {
      if (tasks.waiting.length > 0) {
        fs.writeFileSync(tasksFile, JSON.stringify({
          tasks: tasks
        }, null, 2));
        console.log(`App will continue scraping after rerunning... (${tasks.waiting.length} tasks saved)`.grey);
      }

      connection.close(function (err) {
        console.timeEnd("App worked: ".grey);
        if (err) throw err;
        process.exit();
      });
    });

  });

}
