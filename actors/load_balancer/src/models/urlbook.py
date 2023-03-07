import os, requests, random

class URLBook:
    def __init__(self):
        self.wr_only_mgr_url = os.environ.get("WR_ONLY_MGR_URL")
        self.rd_only_mgr_urls = [os.environ.get("RD_ONLY_MGR1_URL"), os.environ.get("RD_ONLY_MGR2_URL")]

    def get_wr_url(self):
        return self.wr_only_mgr_url
    
    def get_rd_url(self, index):
        if len(self.rd_only_mgr_urls) <= index:
            raise IndexError("Index out of bounds")
        return self.rd_only_mgr_urls[index]
    
    def get_random_live_rd_url(self):
        live_urls = []
        for url in self.rd_only_mgr_urls:
            if self.is_live(url):
                live_urls.append(url)
        return live_urls[random.randint(0, len(live_urls))]
        
    def is_live(url):
        try:
            response = requests.get(url)
            if response.status_code != 200:
                return False
            else:
                return True
        except Exception as e:
            return False
