from traveler import LinksBFS

def main():

    urls = ['https://en.wikipedia.org/wiki/United_States_at_the_2016_Summer_Olympics',
            'https://en.wikipedia.org/wiki/Great_Britain_at_the_2016_Summer_Olympics',
            'https://en.wikipedia.org/wiki/China_at_the_2016_Summer_Olympics',
            'https://en.wikipedia.org/wiki/Russia_at_the_2016_Summer_Olympics',
            'https://en.wikipedia.org/wiki/Germany_at_the_2016_Summer_Olympics']
    
    for url in urls:
        LinksBFS(url, 2)

if __name__ == "__main__":
    main()
    