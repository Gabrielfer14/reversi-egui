use eframe::egui;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};  // Corrigido aqui
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]  // Clonando GameState
struct GameState {
    board: [[i32; 8]; 8],
    current_turn: i32,
}

struct ReversiGame {
    board: [[i32; 8]; 8],
    current_turn: i32,
    server_url: String,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,  // Tipo mais genérico
}

impl Clone for ReversiGame {
    fn clone(&self) -> Self {
        ReversiGame {
            board: self.board,
            current_turn: self.current_turn,
            server_url: self.server_url.clone(),
            ws_stream: None,  // Não clonamos a conexão WebSocket
        }
    }
}

impl ReversiGame {
    fn new() -> Self {
        ReversiGame {
            board: [[0; 8]; 8],
            current_turn: 1,
            server_url: "ws://127.0.0.1:8080".to_string(),
            ws_stream: None,
        }
    }

    async fn connect_to_server(&mut self) {
        let (ws_stream, _) = connect_async(&self.server_url).await.unwrap();
        println!("Conectado ao servidor");

        self.ws_stream = Some(ws_stream);
    }

    async fn make_move(client: Arc<Mutex<ReversiGame>>, row: usize, col: usize) {
        let mut app = client.lock().await;
        if app.board[row][col] == 0 {
            // Atualizar o tabuleiro localmente
            app.board[row][col] = app.current_turn;
            app.current_turn *= -1; // Alternar turno

            // Enviar a jogada para o servidor
            let state = GameState {
                board: app.board,
                current_turn: app.current_turn,
            };

            if let Some(ws_stream) = &mut app.ws_stream {
                let msg = serde_json::to_string(&state).unwrap();
                let _ = ws_stream.send(Message::Text(msg)).await;
            }
        }
    }

    fn draw_board(&mut self, ctx: &egui::Context, client: Arc<Mutex<ReversiGame>>) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                for row in 0..8 {
                    ui.vertical(|ui| {
                        for col in 0..8 {
                            let button = ui.add(egui::Button::new(format!("{}", self.board[row][col])));
                            if button.clicked() {
                                // Realizar o movimento ao clicar no botão
                                let client_clone = client.clone();
                                tokio::spawn(async move {
                                    ReversiGame::make_move(client_clone, row, col).await;
                                });
                            }
                        }
                    });
                }
            });
        });
    }

    async fn update_game_state(&mut self) {
        // Receber e processar mensagens do servidor
        if let Some(ws_stream) = &mut self.ws_stream {
            while let Some(Ok(msg)) = ws_stream.next().await {
                if let Message::Text(msg_text) = msg {
                    let game_state: GameState = serde_json::from_str(&msg_text).unwrap();
                    println!("Estado do jogo recebido: {:?}", game_state);
                    // Atualizar o estado do jogo
                    self.board = game_state.board;
                    self.current_turn = game_state.current_turn;
                }
            }
        }
    }
}

impl eframe::App for ReversiGame {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let client = Arc::new(Mutex::new(self.clone()));
        self.draw_board(ctx, client.clone());

        // Atualizar o estado do jogo a partir do servidor
        tokio::spawn(async move {
            let mut app = client.lock().await;
            app.update_game_state().await;
        });
    }

    fn on_exit(&mut self, _ctx: &eframe::glow::Context) {
        println!("Saindo do jogo...");
    }
}

#[tokio::main]
async fn main() {
    let app = Arc::new(Mutex::new(ReversiGame::new()));
    let client = app.clone();

    // Conectar ao servidor em segundo plano
    let client_clone = client.clone();
    tokio::spawn(async move {
        let mut app = client.lock().await;
        app.connect_to_server().await;
    });

    eframe::run_native(
        "Reversi Game",
        eframe::NativeOptions::default(),
        Box::new(move |_cc| {
            Box::new(ReversiGame::new())
        }),
    );
}
