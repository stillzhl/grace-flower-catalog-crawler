extern crate log;

use std::collections::VecDeque;
use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::{thread, time};
use std::collections::HashMap;

use rand::Rng;

use bloom::{BloomFilter, ASMS};
use reqwest::Result as ReqResult;
use select::document::Document;
use select::predicate::Name;
use serde_json::{self, json};
use serde::{Deserialize, Serialize};
use url::{self, Url};
use regex::Regex;
use xpather;
use tokio;
use log::{info, debug, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init();

    let mut rng = rand::thread_rng();

    let feed_link = env::var("FEED").unwrap();
    let feed_is_list: bool = env::var("IS_NOT_LIST").is_err();
    let feed = Feed {
        link: feed_link,
        is_list: feed_is_list,
    };
    let mut crawler = Crawler::new(feed);
    let mut filter = BloomFilter::with_rate(0.01, 10000);
    while let Some(feed) = crawler.queue.pop_front() {
        let link = feed.link.as_str();
        let html = crawler.fetch(&link).await?;
        let html = crawler.polish_html(&html);

        if feed.is_list {
            let sub_feeds = crawler.parse_feeds(&html).unwrap();

            for sub_feed in sub_feeds {
                if !filter.contains(&sub_feed) {
                    filter.insert(&sub_feed);
                    crawler.queue.push_back(sub_feed);
                }
            }
        } else {
            let path = crawler.save_html(&link, &html)?;
            match crawler.parse_flower(&link, &path) {
                Ok(flower) => {
                    match crawler.save_flower(&flower).await {
                        Ok(flw_id) => {
                            info!("Flower {} saved as id {}", flower.flw_name, flw_id);
                        },
                        Err(err_msg) => {
                            error!("Save Flower {} Failed: {}", flower.flw_name, err_msg);
                            match crawler.log_failure(link, FailureReason::SaveFailure) {
                                Ok(_) => continue,
                                Err(err) => error!("Log failure failed: {}", err),
                            }
                        }
                    }
                },
                Err(err_msg) => {
                    error!("Parsing Failed: {}", err_msg);
                    match crawler.log_failure(link, FailureReason::ParseFailure) {
                        Ok(_) => continue,
                        Err(err) => error!("Log failure failed: {}", err),
                    }
                }
            }
        }

        let x_secs = time::Duration::from_secs(rng.gen_range(1..10));
        thread::sleep(x_secs);
    }
    Ok(())
}

#[derive(Debug, Clone, Hash)]
enum FailureReason {
    ParseFailure,
    SaveFailure,
}

#[derive(Debug, Clone, Hash)]
struct Feed {
    link: String,
    is_list: bool,
}

struct Crawler {
    queue: VecDeque<Feed>,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
struct Flower {
    flw_source: String,
    flw_name: String,
    flw_season: String,
    flw_img: String,
    flw_family: String,
    flw_desc: String,
    flw_site_chars: String,
    flw_plant_traits: String,
    flw_special_cons: String,
    flw_growing_infos: String,
    flw_varieties: String,
}

impl Flower {
    async fn save(&self) -> ReqResult<String> {
        let client = reqwest::Client::new();
        let response = client.post("http://localhost:8080/flower").json(self).send().await?;
        Ok(response.text().await?)
    }

    // async fn delete(&self, flw_id: &str) -> ReqResult<String> {
    //     let client = reqwest::Client::new();
    //     let response = client.delete(format!("http://localhost:8080/flower/{}", flw_id)).send().await?;
    //     Ok(response.text().await?)
    // }
}

impl Crawler {
    fn new(feed: Feed) -> Crawler {
        let mut queue: VecDeque<Feed> = VecDeque::new();
        queue.push_front(feed);
        Crawler { queue }
    }

    async fn fetch(&self, link: &str) -> ReqResult<String> {
        Ok(reqwest::get(link).await?.text().await?)
    }

    fn polish_html(&self, html: &str) -> String {
        let re_multi_new_lines = Regex::new(r"\r").unwrap();
        let html = String::from(re_multi_new_lines.replace_all(html, "\n"));
        let re_tab = Regex::new(r"\t").unwrap();
        let html = String::from(re_tab.replace_all(html.as_str(), ""));
        let re_plain_text = Regex::new(r"</(\w+)>\s*\n*\s*([\w,.: -]+)\n*\s*<").unwrap();
        let html = String::from(re_plain_text.replace_all(html.as_str(), "</$1>\n<p>$2</p><"));
        String::from(html)
    }

    fn parse_feeds(&self, html: &str) -> Result<Vec<Feed>, String> {
        let feeds = Document::from(html)
            .find(Name("a"))
            .filter_map(|n| n.attr("href"))
            .filter(|link| link.starts_with("scene"))
            .map(|link| Feed {
                link: format!(
                    "http://www.gardening.cornell.edu/homegardening/{}",
                    link.to_string()
                ),
                is_list: false,
            })
            .collect();
        Ok(feeds)
    }

    fn parse_flower(&self, link: &str, path: &str) -> Result<Flower, String> {
        fn parse_name(doc: &xpather::Document) -> String {
            debug!("Parsing NAME");
            let flower_name = doc
                .evaluate("//div[@class='head2']/p/b/text()")
                .expect("XPath evaluation failed");
            let flower_name = flower_name.vec_string().unwrap().pop().unwrap().trim().to_string();
            flower_name
        }

        fn parse_img(doc: &xpather::Document) -> String {
            debug!("Parsing IMG");
            let flower_img = doc
                .evaluate("//div[@class='caption']/a/@href")
                .expect("XPath evaluation failed");
            let flower_img = flower_img.vec_string().unwrap_or(vec![]).pop().unwrap_or("".to_string()).trim().to_string();
            let flower_img = format!("http://www.gardening.cornell.edu/homegardening/{}", flower_img);
            flower_img
        }

        fn parse_field(doc: &xpather::Document, xpath: &str, field: &str) -> String {
            debug!("Parsing Field {}", field);
            let re = Regex::new(r"\n|\t").unwrap();
            let re_2spaces = Regex::new(r" {2,}").unwrap();
            let field = doc
                .evaluate(xpath)
                .expect("XPath evaluation failed");
            let field: Vec<String> = field
                .vec_string()
                    .unwrap()
                .iter()
                .map(|item| item.trim())
                .map(|item| re.replace_all(item, "").to_string())
                .map(|item| re_2spaces.replace_all(item.as_str(), " ").to_string())
                .filter(|item| item != "")
                .collect();
            let field = json!(field).to_string();
            debug!("Parsing Field {} Finished!!!!", field);
            field
        }

        info!("Parsing link {}", link);
        let doc = xpather::parse_doc(&mut File::open(path).expect("File::open"));
        let detail_sections = doc.evaluate("//div[@class='intro']/a/text()").expect("XPath eva failed");
        let n_detail_sections = detail_sections.vec_string().unwrap().len();
        debug!("Detail sections: {}", n_detail_sections);
        let flower = Flower {
            flw_source: String::from(link),
            flw_name: parse_name(&doc),
            flw_img: parse_img(&doc),
            flw_season: parse_field(&doc, "//div[@class='normal']/p[1]//text()", "season"),
            flw_family: parse_field(&doc, "//div[@class='normal']/p[2]//text()", "family"),
            flw_desc: parse_field(&doc, "//div[@class='normal']/p[3]//text()", "desc"),
            flw_site_chars: trans_list_to_map(parse_field(&doc,
                format!("//div[@class='intro'][{}]//text()", n_detail_sections+2).as_str(), "site_chars"))?,
            flw_plant_traits: trans_list_to_map(parse_field(&doc,
                format!("//div[@class='intro'][{}]//text()", n_detail_sections+4).as_str(), "plant_traits"))?,
            flw_special_cons: trans_list_to_map(parse_field(&doc,
                format!("//div[@class='intro'][{}]//text()", n_detail_sections+6).as_str(), "special_cons"))?,
            flw_growing_infos: trans_list_to_map(parse_field(&doc,
                format!("//div[@class='intro'][{}]//text()", n_detail_sections+8).as_str(), "growing_infos"))?,
            flw_varieties: parse_field(&doc,
                format!("//div[@class='intro'][{}]//text()", n_detail_sections+10).as_str(), "varieties")
        };
        Ok(flower)
    }

    fn save_html(&self, link: &str, html: &str) -> std::io::Result<String> {
        fn parse_basename(link: &str) -> Result<String, url::ParseError> {
            let link_url = Url::parse(link)?;
            let path_segments = link_url
                .path_segments()
                .map(|s| s.collect::<Vec<_>>())
                .unwrap();
            let length = path_segments.len();
            Ok(String::from(path_segments[length - 1]))
        }

        let basename = parse_basename(link).unwrap();
        let path = String::from(format!("html/{}", basename));
        let mut file = File::create(&path).unwrap();

        write!(file, "<link>{}</link>\n{}", link, html)?;

        Ok(path)
    }

    async fn save_flower(&self, flower: &Flower) -> ReqResult<String> {
        let flw_json = flower.save().await;
        match flw_json {
            Ok(flw_json) => {
                let flw_json: HashMap<String, String> = serde_json::from_str(flw_json.as_str()).unwrap();
                let flw_id = flw_json.get("flw_id").unwrap();
                Ok(String::from(flw_id))
            },
            Err(err_msg) => {
                Err(err_msg)
            }
        }
    }

    fn log_failure(&self, link: &str, reason: FailureReason) -> std::io::Result<()> {
        let mut f_link_failures = OpenOptions::new()
            .append(true)
            .open("log/link_failures.txt")?;
        write!(f_link_failures, "{},{:?}\n", link, reason)?;
        Ok(())
    }
}

fn trans_list_to_map(lst: String) -> Result<String, String> {
    let lst: Vec<String> = serde_json::from_str(lst.as_str()).unwrap();
    let re = Regex::new(r":$").unwrap();
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    let mut i = 0;
    let mut n = 0;
    loop {
        n = n + 1;
        let item = lst[i].clone();
        if item.ends_with(":") {
            let mut values: Vec<String> = Vec::new();
            loop {
                if i >= lst.len() - 1 {
                    break;
                }
                i = i + 1;
                let next_item = lst[i].clone();
                if next_item.ends_with(":") {
                    break;
                } else {
                    values.push(next_item);
                }
            }
            let trimmed_item = String::from(re.replace_all(&item, ""));
            m.insert(trimmed_item, values);
        }
        if i >= lst.len() - 1 {
            break;
        }

        if n > 100 {
            return Err("Dead Loop".to_string());
        }
    }
    Ok(json!(m).to_string())
}


#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;

    // #[test]
    // fn test_save_flower() {
    //     let crawler = Crawler::new("");
    //     let flower = Flower {
    //         flw_source: String::from("http://www.gardening.cornell.edu/homegardening/scenecea9.html"),
    //         flw_name: String::from("Astilbe, Chinese"),
    //         flw_img: String::from("http://www.gardening.cornell.edu/homegardening/images/garden/photos_garden/Saxifragaceae/Astilbe/chinensis/whole.jpg"),
    //         flw_family: String::from("Herbaceous Perennial Flower"),
    //         flw_season: String::from("Summer"),
    //         flw_desc: String::from("Deceptively delicate in appearance this moisture- and sem"),
    //         flw_site_chars: String::from("Prefers shady, moist sites, but needs good drainage over winter."),
    //         flw_plant_traits: String::from("Most varieties grow about 1.5 to 3 feet tall. 'Davidii' grows up to 6 feet tall."),
    //         flw_special_cons: String::from("bears ornamental fruit - Spent flowers are attractive and can be left until spring."),
    //         flw_growing_infos: String::from("Propagate by division"),
    //         flw_varieties: String::from("var. davidii grows 3 to 6 feet tall with deep green leaves heavily"),
    //     };
    //     let flw_id = crawler.save_flower(&flower).await?;
    //     assert_eq!(true, flw_id.is_ok());
    //     if let Some(uid) = flw_id {
    //         flower.delete(uid).await?;
    //     }
    // }
    #[test]
    fn test_log_failure() {
        let crawler = Crawler::new(Feed {link: "".to_string(), is_list: false});
        let link = "http://aaa.com";
        let res = crawler.log_failure(link, FailureReason::ParseFailure).unwrap();
        assert_eq!((), res);
    }

    #[test]
    fn test_trans_list_to_map() {
        let s = r#"["A:", "1", "2", "B:", "3", "C:", "4", "5", "6"]"#;
        let expected = r#"{"A":["1","2"],"B":["3"],"C":["4","5","6"]}"#;
        assert_eq!(expected.to_string(), trans_list_to_map(s.to_string()).unwrap());
    }

    #[test]
    fn test_reg_replace_blanks() {
        let re = Regex::new(r"\n|\t").unwrap();
        let s = " \n\n\n\n\n\n\t\taaa";
        assert_eq!(" aaa", re.replace_all(s, ""));
    }

    #[test]
    fn test_reg_replace_multi_new_lines() {
        let re = Regex::new(r"\n{1,}").unwrap();
        let s = " \n\n\naaa";
        assert_eq!(" \naaa", re.replace_all(s, "\n"));
    }

    #[test]
    fn test_reg_wrap_plain_text() {
        let re = Regex::new(r"</(\w+)>\s*\n*\s*([\w,.: -]+)\n*\s*<").unwrap();
        let s = "</ul>







	Prefers a light, well-drained soil.















	</p>
<b>Shape:</b>  





























		upright</p>";
        assert_eq!("</ul>\n<p>Prefers a light, well-drained soil.</p></p>\n<b>Shape:</b>\n<p>upright</p></p>", re.replace_all(s, "</$1>\n<p>$2</p><"));
    }
}
