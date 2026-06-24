# sophon-tools

High-performance async sophon downloader implementation written in Rust.

<table>
    <tr>
        <td><img src="repository/list-games.png" height="384px" /></td>
        <td><img src="repository/list-game-components.png" height="384px" /></td>
    </tr>
    <tr>
        <td><img src="repository/game-versions.png" height="384px" /></td>
        <td><img src="repository/game-download-info.png" height="384px" /></td>
    </tr>
</table>

# Features

- Async-native
- Pure rust and musl-friendly, no C bindings used
- Simple API interactions
- Files verification for any game version and component, with different modes
- Smart files downloading for any game version and component with no disk
  cache writes and proxy support

Licensed under [GPL-3.0-or-later](LICENSE)
