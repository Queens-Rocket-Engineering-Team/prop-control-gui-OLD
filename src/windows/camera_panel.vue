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
            arr.push(`${camera_url}:8889${element.stream_path}`)
        });
        text.value = "Cameras Loaded Successfully";
    }).catch(error => {
        text.value = error;
    })
}

</script>

<template>
    <div>
        <p>BLAH {{ text }}</p>
        <h1>Camera View</h1>
        <div className="camera_control" v-for="(item, index) in arr" :key="index" >
            
            <iframe :src="item" width="400" />
            <button>left</button>
            <button>right</button>
        </div>
        <button @click="get_list">get</button>
        <p>Cameras Output: {{ text }}</p>
    </div>
</template>