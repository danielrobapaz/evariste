from extract_content_countries import ExtractContentFromWikipedia


def main():
    extract = ExtractContentFromWikipedia()
    url = "https://en.wikipedia.org/wiki/United_States"
    (_, a) = extract.request_maker(url)
    print(a)
        
if __name__ == "__main__":
    main()
    