from scrapper_manager.extractor import ExtractContentFromWikipedia

class LinksBFS:
    def __init__(self,
                 root_url: str,
                 deep: int = 3) -> None:
        extractor = ExtractContentFromWikipedia()        
        links, content = extractor.request_maker(root_url)
        self.__save_content(content, f"{root_url}.txt")
        current_deep = 1

        while current_deep < deep:
            print(f'Current deep: {current_deep} - {len(links)} links')
            
            new_links = set()
            for (i, link) in enumerate(links):
                if i % 20 == 0:
                    print(f'{i}/{len(links)} links')
                (current_links, content) = extractor.request_maker(link)
                new_links = new_links.union(current_links)
                self.__save_content(content, f"{link}.txt")
            
            current_deep += 1
            links = new_links


    def __save_content(self,
                       content: str,
                       file_name: str) -> None:
        
        try:
            file_name = file_name.replace("/", "_")
            with open(f'../documents/{file_name}', 'x+') as file:
                file.write(content)

        except Exception as e:
            print(e)