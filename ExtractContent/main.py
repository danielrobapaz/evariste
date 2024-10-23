from links_bfs import LinksBFS

def main():

    urls = ["https://en.wikipedia.org/wiki/Japan_at_the_2024_Summer_Olympics",
            "https://en.wikipedia.org/wiki/China_at_the_2024_Summer_Olympics",
            "https://en.wikipedia.org/wiki/France_at_the_2024_Summer_Olympics",
            "https://en.wikipedia.org/wiki/United_States_at_the_2024_Summer_Olympics",
            "https://en.wikipedia.org/wiki/Australia_at_the_2024_Summer_Olympics"]
    
    url = urls[4]
    LinksBFS(url, 2)

if __name__ == "__main__":
    main()
    