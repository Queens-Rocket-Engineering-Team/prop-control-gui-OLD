// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
use std::string::String;
use std::sync::Mutex;

static IP_ADDRESS: Mutex<String> = Mutex::new(String::new());

#[tauri::command]
// returns the current ip address
async fn fetch_server_ip() -> String {
    let gaurded_ip = IP_ADDRESS.lock().unwrap();
    gaurded_ip.to_string()
}


#[tauri::command]
//stores the inputted string in IP_ADDRESS for later use
async fn submit_ip(new_ip: String) {
    let mut gaurded_ip = IP_ADDRESS.lock().unwrap();
    println!("New IP Submitted: {}", new_ip);
    *gaurded_ip = String::from(new_ip);
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![fetch_server_ip, submit_ip])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
