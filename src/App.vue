<script setup>
import { computed, onMounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";

//import the windows as they are defined in their own vue files
import WelcomePanel from "./windows/welcome_panel.vue";
import CameraPanel from "./windows/camera_panel.vue";
import GraphPanel from "./windows/graph_panel.vue";
import ControlPanel from "./windows/control_panel.vue";
import DebugPanel from "./windows/debug_panel.vue";
import FlightPanel from "./windows/flight_panel.vue";

//import complex components to clean up code.
//to note, if componetns are to be used in the <template> below, they must be imported 
//in pascal case, and then used in kebab-case.
import ServerBar from "./components/server_bar.vue";


const window_content = ref(WelcomePanel); //initializes the functional window content to the welcome panel.
//changes the content of the functional window to the inputted compnent
function setActive(component) {
  window_content.value = component;
}

//collapse or expand navbar on button press.
//TODO: Use jquery or something to make it prettier
function collapseNavbar() {
  const collapseDiv = document.getElementById("collapse");
  if (collapseDiv.style.display === "none" || collapseDiv.style.display === "") {
    collapseDiv.style.display = "block";
  } else {
    collapseDiv.style.display = "none";
  }
}

const server_ip = ref("");
function get_ip(new_ip) {
  console.log("Received new IP from server bar:", new_ip);
  server_ip.value = new_ip;
}

</script>

<template>
  <main class="container">
    <div id="grid-container">
        <div id="navbar" >
          <div id="menu-button" @click="collapseNavbar()">
            <img src="./assets/dropdown.svg" width="30px" />
          </div>
          <div id="collapse">
            <div id="nav-upper">
              <button @click="setActive(ControlPanel)">Control</button>
              <button @click="setActive(GraphPanel)">Data</button>
              <button @click="setActive(CameraPanel)">Camera View</button>
              <button @click="setActive(DebugPanel)">Debug</button>
              <button @click="setActive(FlightPanel)">Flight</button>
            </div>
            <div id="nav-lower">
              <button @click="setActive(WelcomePanel)">Welcome</button>
            </div>
          </div>
        </div>
        <component :is="window_content" class="swap-container"></component>
        
      </div>
      <server-bar @update-ip="get_ip"></server-bar>
  </main>
</template>


