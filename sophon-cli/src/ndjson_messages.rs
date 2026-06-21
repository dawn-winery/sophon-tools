use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum NdJsonMessage<'a> {
    Message(&'a str),
    Failure(&'a str),
    DownloadInfo(&'a sophon_lib::api::schemas::sophon_manifests::SophonDownloads),
    Download(sophon_lib::installer::Update),
    RepairInfo(&'a sophon_lib::api::schemas::sophon_manifests::SophonDownloads),
    Repair(sophon_lib::installer::Update),
    UpdateInfo(&'a sophon_lib::api::schemas::sophon_diff::SophonDiffs),
    Update(sophon_lib::updater::Update),
}

impl<'a> NdJsonMessage<'a> {
    pub fn msg(msg: &'a str) -> Self {
        Self::Message(msg)
    }

    pub fn fail(msg: &'a str) -> Self {
        Self::Failure(msg)
    }

    pub fn print(&self) {
        println!(
            "{}",
            serde_json::to_string(self).expect("Failed to serialize message")
        )
    }

    pub fn dlmsg(msg: sophon_lib::installer::Update) -> Self {
        Self::Download(msg)
    }

    pub fn repmsg(msg: sophon_lib::installer::Update) -> Self {
        Self::Repair(msg)
    }

    pub fn updmsg(msg: sophon_lib::updater::Update) -> Self {
        Self::Update(msg)
    }
}
