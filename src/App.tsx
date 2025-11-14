import { useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import "./App.css";

function App() {

  const [content, setContent] = useState<JSX.Element>(<p>default</p>);

  const control_panel = <div >Control Panel</div>;
  const graph_panel = <div>Graphs</div>;

  const camera_panel = <div>
    <h1>Camera View</h1>
    <div className="camera_control">
      <iframe width="1200" height="800" src="http://192.168.1.5:8889/cam1/" />
      <button onClick={() => fetch("http://192.168.1.5:8000/v1/camera?ip=192.168.1.6&x_movement=0.2&y_movement=0.0", { method: "POST", headers: { "Authorization": `Basic ${btoa("admin:propteambestteam")}` }}).then(r => console.log (r)).catch(e => console.log(e))}>left</button>
      <button onClick={() => fetch("http://192.168.1.5:8000/v1/camera?ip=192.168.1.6&x_movement=-0.2&y_movement=0.0", { method: "POST", headers: { "Authorization": `Basic ${btoa("admin:propteambestteam")}` }}).then(r => console.log (r)).catch(e => console.log(e))}>right</button>
    </div>
    </div>;

  return (
    <main className="container">

      <div className="grid-container">
        <div className="navbar" >
          <button onClick={() => setContent(control_panel)}>Controls</button>
          <button onClick={() => setContent(graph_panel)}>Monitoring</button>
          <button onClick={() => setContent(camera_panel)}>Camera View</button>
        </div>
        <div className="swap-container">
          {content}
        </div>
      </div>
      
    </main>
  );
}

export default App;
