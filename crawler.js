const axios = require("axios");
const cheerio = require("cheerio");
const { URL } = require("url");
const fs = require("fs");
const path = require("path");

const calculatePageRank = require("./pageRank");

const START_URLS = [
  "https://www.youm7.com/",
  "https://www.almasryalyoum.com/",
  "https://www.masrawy.com/",
  "https://www.bbc.com/",
  "https://cnn.com/",
  "https://www.ajnet.me/",
];
const MAX_PAGES = 1000;
const CONTENT_DIR = path.join(__dirname, "content");

if (!fs.existsSync(CONTENT_DIR)) {
  fs.mkdirSync(CONTENT_DIR);
}

let pagesToVisit = [...START_URLS];
let visitedPages = {};
let linksGraph = {};
let fileIndex = 1;
let urlToFileMap = {};

async function fetchPage(url) {
  try {
    const response = await axios.get(url, { timeout: 3000 });
    return cheerio.load(response.data);
  } catch (error) {
    console.error(`Error fetching ${url}: ${error.message}`);
    return null;
  }
}

function saveContent(url, content) {
  const fileName = `${fileIndex}.txt`;
  const filePath = path.join(CONTENT_DIR, fileName);
  try {
    fs.writeFileSync(filePath, content, "utf8");
    urlToFileMap[fileName] = url;
    fileIndex++;
  } catch (err) {
    console.error(`Error saving content for ${url}: ${err.message}`);
  }
}

async function processPage(url) {
  if (visitedPages[url]) return;

  console.log(`Visiting: ${url}`);
  const $ = await fetchPage(url);
  if (!$) return;

  visitedPages[url] = true;
  linksGraph[url] = [];

  $("script").remove();
  $("style").remove();
  $("noscript").remove();
  $("meta").remove();
  $("link").remove();
  $("img").remove();
  $("iframe").remove();
  $("svg").remove();
  $("video").remove();
  $("audio").remove();
  $("object").remove();
  $("embed").remove();
  $("applet").remove();
  $("area").remove();
  $("map").remove();
  $("param").remove();
  $("track").remove();
  $("source").remove();
  $("canvas").remove();
  $("math").remove();
  textContent = $.text();
  textContent = textContent.replace(/\{.*?\}/g, "");
  textContent = textContent.replace(/\s+/g, " ");
  saveContent(url, textContent);

  $("a[href]").each((i, link) => {
    const href = $(link).attr("href");
    try {
      const absoluteUrl = new URL(href, url).href;
      if (!visitedPages[absoluteUrl] && !pagesToVisit.includes(absoluteUrl)) {
        pagesToVisit.push(absoluteUrl);
        linksGraph[url].push(absoluteUrl);
      }
    } catch (err) {
      console.error(`Error parsing URL ${href} on ${url}: ${err.message}`);
    }
  });
  return;
}

async function crawl() {
  while (pagesToVisit.length && Object.keys(visitedPages).length < MAX_PAGES) {
    const promises = pagesToVisit.splice(0, 5).map((url) => processPage(url));
    await Promise.all(promises);
  }

  console.log("Crawling finished.");

  fs.writeFile(
    "links_graph.json",
    JSON.stringify(linksGraph, null, 2),
    (err) => {
      if (err) {
        console.error("Error writing links graph to file:", err);
      } else {
        console.log("Links graph saved to links_graph.json.");
      }
    }
  );

  fs.writeFile(
    "url_to_file_map.json",
    JSON.stringify(urlToFileMap, null, 2),
    (err) => {
      if (err) {
        console.error("Error writing URL to file map:", err);
      } else {
        console.log("URL to file map saved to url_to_file_map.json.");
      }
    }
  );

  calculatePageRank(linksGraph);
}

crawl();
