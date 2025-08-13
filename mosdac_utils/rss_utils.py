import pandas as pd
import datetime
import feedparser

def rss_scraper(url):
    try:
        feed = feedparser.parse(url)

        if not feed.entries:
            print("No entries found in feed")
            return
        
        all_data = []

        for entry in feed.entries:
            title = entry.get("title")
            description = entry.get("description")
            pub_date = entry.get("published")
            acq_start = entry.get("datacasting_acquisitionstartdate")

            acq_dt = datetime.datetime.strptime(acq_start, "%a, %d %b %Y %H:%M:%S %Z")
            acq_str = acq_dt.strftime("%Y-%m-%dT%H-%M-%S")  

            product_id = f"{title}_{acq_str}"

            preview_url = entry.get("datacasting_preview")
            link = entry.get("link")
            bbox_lower = entry.get("gml_lowercorner")
            bbox_upper = entry.get("gml_uppercorner")

            if bbox_lower:
                bbox_lower = list(map(float, bbox_lower.split())) 
            if bbox_upper:
                bbox_upper = list(map(float, bbox_upper.split()))
            
            data = {
                "product_id":product_id,
                "title":title,
                "description":description,
                "pub_date":pub_date,
                "acq_start":acq_start,
                "preview_url":preview_url,
                "link":link,
                "bbox_lower":bbox_lower,
                "bbox_upper":bbox_upper
            }
            
            all_data.append(data)

        df = pd.DataFrame(all_data)
        print("df sucessfully created.")
        return df
    except Exception as e:
        print("error: ",e)