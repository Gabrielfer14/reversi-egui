use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, Deserialize};
use std::io::{self, Write};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GameState {
    board: [[i32; 8]; 8],
    current_turn: i32,
}

struct ReversiGame {
    board: [[i32; 8]; 8],
    current_turn: i32,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl ReversiGame {
    fn new() -> Self {
        ReversiGame {
            board: [[0; 8]; 8],
            current_turn: 1,
            ws_stream: None,
        }
    }

    async fn connect_to_server(&mut self, server_url: &str) {
        match connect_async(server_url).await {
            Ok((ws_stream, _)) => {
                println!("Conectado ao servidor");
                self.ws_stream = Some(ws_stream);
            },
            Err(e) => {
                println!("Erro ao conectar ao servidor: {}", e);
            }
        }
    }

    async fn send_move(&mut self, row: usize, col: usize) {
        if self.board[row][col] == 0 {
            self.board[row][col] = self.current_turn;
            self.current_turn *= -1;

            let state = GameState {
                board: self.board,
                current_turn: self.current_turn,
            };

            if let Some(ws_stream) = &mut self.ws_stream {
                let msg = serde_json::to_string(&state).unwrap();
                if let Err(e) = ws_stream.send(Message::Text(msg)).await {
                    println!("Erro ao enviar mensagem: {}", e);
                }
            }
        }
    }

    async fn receive_game_state(&mut self) {
        if let Some(ws_stream) = &mut self.ws_stream {
            let mut game_state: Option<GameState> = None; // Armazenar o estado do jogo temporariamente
    
            // Ler a mensagem e processar
            while let Some(Ok(msg)) = ws_stream.next().await {
                if let Message::Text(msg_text) = msg {
                    game_state = Some(serde_json::from_str(&msg_text).unwrap());
                }
            }
    
            // Quando o loop terminar, usamos o estado do jogo se foi recebido
            if let Some(state) = game_state {
                self.board = state.board;
                self.current_turn = state.current_turn;
                self.print_board(); // Atualiza o tabuleiro no cliente
            }
        }
    }
    
    fn print_board(&self) {
        println!("Estado atual do jogo:");
        for row in 0..8 {
            for col in 0..8 {
                let symbol = match self.board[row][col] {
                    1 => "X",
                    -1 => "O",
                    _ => ".",
                };
                print!("{} ", symbol);
            }
            println!();
        }
        println!("Vez do jogador: {}", if self.current_turn == 1 { "X" } else { "O" });
    }

    fn prompt_move(&self) -> (usize, usize) {
        loop {
            print!("Digite sua jogada (linha e coluna): ");
            io::stdout().flush().unwrap();
            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();
            let parts: Vec<&str> = input.trim().split_whitespace().collect();
            if parts.len() == 2 {
                if let (Ok(row), Ok(col)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
                    if row < 8 && col < 8 && self.board[row][col] == 0 {
                        return (row, col);
                    }
                }
            }
            println!("Entrada inválida. Tente novamente.");
        }
    }
}

#[tokio::main]
async fn main() {
    let mut game = ReversiGame::new();

    // Conectar ao servidor
    game.connect_to_server("ws://127.0.0.1:8080").await;

    loop {
        // Exibir o tabuleiro e perguntar pela jogada
        game.print_board();
        let (row, col) = game.prompt_move();

        // Enviar a jogada para o servidor
        game.send_move(row, col).await;

        // Receber e exibir o estado atualizado do jogo
        game.receive_game_state().await;
    }
}
