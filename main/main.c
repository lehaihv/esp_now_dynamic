#include <stdio.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "esp_wifi.h"
#include "esp_now.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_mac.h"
#include "esp_log.h"
#include "nvs_flash.h"

static const char *TAG = "ESPNOW_RTOS";

// ==========================================
// CONFIGURATION
// ==========================================
#define MSG_DISCOVERY         0
#define MSG_DATA              1
#define DISCOVERY_INTERVAL_MS 5000
#define MAX_PEERS             10
#define RX_QUEUE_SIZE         10

#define MAC_FMT "%02x:%02x:%02x:%02x:%02x:%02x"
#define MAC_ARG(m) (m)[0], (m)[1], (m)[2], (m)[3], (m)[4], (m)[5]

// ==========================================
// DATA STRUCTURES
// ==========================================
typedef struct __attribute__((packed)) {
    uint8_t msgType;
    uint8_t srcMac[6];
    int payload;
    char text[32];
} Message;

typedef struct {
    uint8_t mac[6];
    Message msg;
} rx_queue_item_t;

// ==========================================
// GLOBAL STATE & RTOS PRIMITIVES
// ==========================================
static uint8_t knownPeers[MAX_PEERS][6];
static int peerCount = 0;
static QueueHandle_t rx_queue;
static SemaphoreHandle_t peers_mutex;

// ==========================================
// FORWARD DECLARATIONS
// ==========================================
static void esp_now_recv_cb(const esp_now_recv_info_t *recv_info, const uint8_t *data, int data_len);
static void esp_now_send_cb(const wifi_tx_info_t *tx_info, esp_now_send_status_t status);
static void recv_task(void *pvParameters);
static void send_task(void *pvParameters);

// ==========================================
// WI-FI INITIALIZATION
// ==========================================
static void wifi_init_sta(void) {
    esp_netif_init();
    esp_event_loop_create_default();
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_start());
    
    // CRITICAL FIX 1: Explicitly lock the Wi-Fi channel when not connected to an AP.
    // ESP-NOW requires both devices to be on the exact same channel.
    ESP_ERROR_CHECK(esp_wifi_set_channel(1, WIFI_SECOND_CHAN_NONE));
}

// ==========================================
// RTOS TASKS
// ==========================================

static void recv_task(void *pvParameters) {
    ESP_LOGI(TAG, "recv_task started");
    rx_queue_item_t item;
    
    while (1) {
        if (xQueueReceive(rx_queue, &item, portMAX_DELAY) == pdTRUE) {
            xSemaphoreTake(peers_mutex, portMAX_DELAY);
            
            if (item.msg.msgType == MSG_DISCOVERY) {
                bool known = false;
                for (int i = 0; i < peerCount; i++) {
                    if (memcmp(knownPeers[i], item.msg.srcMac, 6) == 0) {
                        known = true;
                        break;
                    }
                }
                
                if (!known && peerCount < MAX_PEERS) {
                    memcpy(knownPeers[peerCount], item.msg.srcMac, 6);
                    
                    esp_now_peer_info_t peerInfo = {0};
                    memcpy(peerInfo.peer_addr, item.msg.srcMac, 6);
                    peerInfo.channel = 0;
                    peerInfo.ifidx = WIFI_IF_STA;
                    peerInfo.encrypt = false;
                    
                    if (esp_now_add_peer(&peerInfo) == ESP_OK) {
                        ESP_LOGI(TAG, "[+] Discovered & added peer: " MAC_FMT, MAC_ARG(item.msg.srcMac));
                        peerCount++;
                    } else {
                        ESP_LOGE(TAG, "Failed to add peer: " MAC_FMT, MAC_ARG(item.msg.srcMac));
                    }
                }
            } 
            else if (item.msg.msgType == MSG_DATA) {
                ESP_LOGI(TAG, "[Data] From " MAC_FMT " | Payload: %d | Text: %s",
                         MAC_ARG(item.mac), item.msg.payload, item.msg.text);
            }
            
            xSemaphoreGive(peers_mutex);
        }
    }
}

static void send_task(void *pvParameters) {
    ESP_LOGI(TAG, "send_task started");
    uint32_t lastDiscovery = 0;
    int counter = 0;
    Message tx_msg;
    
    esp_read_mac(tx_msg.srcMac, ESP_MAC_WIFI_STA);
    ESP_LOGI(TAG, "My MAC: " MAC_FMT, MAC_ARG(tx_msg.srcMac));

    while (1) {
        uint32_t now = xTaskGetTickCount() * portTICK_PERIOD_MS;

        // 1. Broadcast Discovery Packet periodically
        if (now - lastDiscovery >= DISCOVERY_INTERVAL_MS) {
            lastDiscovery = now;
            tx_msg.msgType = MSG_DISCOVERY;
            uint8_t broadcast[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
            
            // Added return value checking to catch silent failures
            esp_err_t result = esp_now_send(broadcast, (uint8_t *)&tx_msg, sizeof(tx_msg));
            if (result != ESP_OK) {
                ESP_LOGE(TAG, "Failed to send discovery: %s", esp_err_to_name(result));
            } else {
                ESP_LOGI(TAG, "Discovery packet sent to broadcast");
            }
        }

        // 2. Send Data to all known peers
        xSemaphoreTake(peers_mutex, portMAX_DELAY);
        
        if (peerCount > 0) {
            tx_msg.msgType = MSG_DATA;
            tx_msg.payload = counter++;
            snprintf(tx_msg.text, sizeof(tx_msg.text), "Hello from " MAC_FMT, MAC_ARG(tx_msg.srcMac));

            for (int i = 0; i < peerCount; i++) {
                esp_err_t res = esp_now_send(knownPeers[i], (uint8_t *)&tx_msg, sizeof(tx_msg));
                if (res != ESP_OK) {
                    ESP_LOGE(TAG, "Failed to send data to peer %d: %s", i, esp_err_to_name(res));
                }
            }
            ESP_LOGI(TAG, "[-] Sent data to %d peers. Counter: %d", peerCount, tx_msg.payload);
        }
        
        xSemaphoreGive(peers_mutex);

        vTaskDelay(pdMS_TO_TICKS(2000));
    }
}

// ==========================================
// ESP-NOW CALLBACKS
// ==========================================

static void esp_now_send_cb(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
    (void)tx_info;
    if (status == ESP_NOW_SEND_SUCCESS) {
        // ESP_LOGD(TAG, "Packet sent successfully");
    } else {
        ESP_LOGW(TAG, "Packet send failed (callback)");
    }
}

static void esp_now_recv_cb(const esp_now_recv_info_t *recv_info, const uint8_t *data, int data_len) {
    if (data_len != sizeof(Message)) return;

    rx_queue_item_t item;
    memcpy(item.mac, recv_info->src_addr, 6);
    memcpy(&item.msg, data, sizeof(Message));

    if (xQueueSend(rx_queue, &item, 0) != pdTRUE) {
        ESP_LOGW(TAG, "RX Queue full, dropping packet");
    }
}

// ==========================================
// MAIN ENTRY POINT
// ==========================================
void app_main(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    wifi_init_sta();

    rx_queue = xQueueCreate(RX_QUEUE_SIZE, sizeof(rx_queue_item_t));
    peers_mutex = xSemaphoreCreateMutex();
    configASSERT(rx_queue);
    configASSERT(peers_mutex);

    ESP_ERROR_CHECK(esp_now_init());
    esp_now_register_send_cb(esp_now_send_cb);
    esp_now_register_recv_cb(esp_now_recv_cb);

    // CRITICAL FIX 2: In raw ESP-IDF, you MUST add the broadcast address as a peer 
    // if you want to send packets to it. The Arduino wrapper hides this requirement.
    esp_now_peer_info_t broadcast_peer = {0};
    uint8_t broadcast_mac[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    memcpy(broadcast_peer.peer_addr, broadcast_mac, 6);
    broadcast_peer.channel = 0;
    broadcast_peer.ifidx = WIFI_IF_STA;
    broadcast_peer.encrypt = false;
    ESP_ERROR_CHECK(esp_now_add_peer(&broadcast_peer));
    ESP_LOGI(TAG, "Broadcast peer added");

    xTaskCreate(recv_task, "recv_task", 4096, NULL, 5, NULL);
    xTaskCreate(send_task, "send_task", 4096, NULL, 5, NULL);

    ESP_LOGI(TAG, "System Ready. RTOS Tasks running.");
}