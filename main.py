import pathlib

from src.parser.parser_logs import ParserLogs
from src.visualizer import DashApp


def main() -> None:
    paths = [p for p in pathlib.Path("logs/").iterdir()]
    parser = ParserLogs(paths)
    data = parser.parse()
    app = DashApp(data)
    app.run(debug=True)


if __name__ == "__main__":
    main()
