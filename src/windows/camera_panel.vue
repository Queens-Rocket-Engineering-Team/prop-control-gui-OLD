<script setup>
import { invoke } from "@tauri-apps/api/core";
import { ref} from "vue";

import { Panel } from "primevue";

let server_ip;
const cameras = ref();

let arr = [];

const text = ref();

async function get_list() {
    arr = []; // empty array on call
    invoke("fetch_server_ip").then((ip) => {
        server_ip = ip;
    });
    text.value = "Fetching Cameras...";
    const camera_url = `http://${server_ip}:8000/v1/cameras`;
    fetch(camera_url, { headers: { "Authorization": `Basic ${btoa("admin:propteambestteam")}`}})    
    .then 
    (
        (res)=>res.json()
    ).then((body) => {
        text.value=JSON.stringify(body);
        cameras.value = body.cameras;
        body.cameras.forEach(element => {
            arr.push(element);
            //arr.push(`${camera_url}:8889${element.stream_path}`)
        });
        text.value = "Cameras Loaded Successfully";
    }).catch(error => {
        text.value = error;
    })
}

function refresh_list() {
    fetch(`http://${server_ip}:8000/v1/cameras/reconnect`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        }
    }).then(_ => {
        get_list();
    });
}

function cam_right(ip) {
    //TODO: update x_movement/y_movement amounts
    fetch(`http://${server_ip}:8000/v1/camera?ip=${ip}&x_movement=-0.2&y_movement=0`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        }
    });
    text.value = "t";
}

function cam_left(ip) { 
    fetch(`http://${server_ip}:8000/v1/camera?ip=${ip}&x_movement=0.2&y_movement=0`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        } 
    });
}

function cam_up(ip) {
    fetch(`http://${server_ip}:8000/v1/camera?ip=${ip}&x_movement=0&y_movement=0.2`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        }
    });
}

function cam_down(ip) {
    fetch(`http://${server_ip}:8000/v1/camera?ip=${ip}&x_movement=0&y_movement=-0.2`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        }
    });
}

</script>

<template>
    <div id="camera_control">
        <h1>Camera View</h1>
        <button @click="get_list">get</button>
        <button @click="refresh_list">refresh</button>
        <p>Cameras Output: {{ text }}</p>
        <div v-for="(item, index) in arr" :key="index" >
            <Panel :header="item.hostname" toggleable="true">
                <iframe :src="`http://${server_ip}:8889${item.stream_path}?autoplay=true`" width="800" height="568" /> 
                <br>
                <button @click="cam_left(item.ip)">left</button>
                <button @click="cam_right(item.ip)">right</button>
                <button @click="cam_up(item.ip)">up</button>
                <button @click="cam_down(item.ip)">down</button>
            </Panel>
        </div>
        
        
    </div>
</template>

<style scoped>
    #camera_control {
        overflow: scroll;
    }
</style>