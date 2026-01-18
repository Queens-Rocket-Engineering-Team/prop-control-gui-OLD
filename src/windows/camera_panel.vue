<script setup>
import { invoke } from "@tauri-apps/api/core";
import { ref, onMounted} from "vue";

let server_ip;
const cameras = ref();

let arr = [];

async function get_list() {
    invoke("fetch_server_ip").then((ip) => {
        server_ip = ip;
    });
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
    fetch(`${ip}:8000/v1/camera?ip=${ip}&x_movement=0.2&y_movement=0`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        }
    });
}

function cam_left(ip) { 
    fetch(`${ip}:8000/v1/camera?ip=${ip}&x_movement=-0.2&y_movement=0`, {
        method: "POST",
        headers: {
            "Authorization": `Basic ${btoa("admin:propteambestteam")}`
        } 
    });
}

function cam_up(ip) {
    fetch(`${ip}:8000/v1/camera?ip=${ip}&x_movement=0&y_movement=0.2`, {
        method: "POST",
        headers: {
            "Atthorization": `Basic ${btoa("admin:propteambestteam")}`
        }
    });
}

function cam_down(ip) {
    fetch(`${ip}:8000/v1/camera?ip=${ip}&x_movement=0&y_movement=-0.2`, {
        method: "POST",
        headers: {
            "Authorization  ": `Basic ${btoa("admin:propteambestteam")}`
        }
    });
}

</script>

<template>
    <div>
        <h1>Camera View</h1>
        <div className="camera_control" v-for="(item, index) in arr" :key="index" >
            <h>{{ item.hostname }}</h>
            <iframe :src="`${camera_url}:8889${item.stream_path}`" width="400" />
            <button @click="cam_right(item.ip)">right</button>
            <button @click="cam_left(item.ip)">left</button>
            <button @click="cam_up(item.ip)">up</button>
            <button @click="cam_down(item.ip)">down</button>
        </div>
        <button @click="get_list">get</button>
        <button @click="refresh_list">refresh</button>
        <p>Cameras Output: {{ text }}</p>
    </div>
</template>