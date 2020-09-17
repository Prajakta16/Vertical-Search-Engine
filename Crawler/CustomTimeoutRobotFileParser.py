from urllib.robotparser import RobotFileParser
import urllib.request
import urllib.robotparser


class CustomRobotFileParser(urllib.robotparser.RobotFileParser):
    def __init__(self, url='', timeout=3):
        super().__init__(url)
        self.timeout = timeout

    def read(self):
        """Reads the robots.txt URL and feeds it to the parser."""
        try:
            f = urllib.request.urlopen(self.url, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                self.disallow_all = True
            elif err.code >= 400:
                self.allow_all = True
        else:
            raw = f.read()
            self.parse(raw.decode("utf-8").splitlines())
