use tokio::{net::TcpListener, io::{self, AsyncWriteExt, AsyncBufReadExt}};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::Mutex;  // Importe o Mutex de Tokio

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GameState {
    board: [[i32; 8]; 8],
    current_turn: i32, // 1 para jogador 1, -1 para jogador 2
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Move {
    row: usize,
    col: usize,
}

struct GameRoom {
    game_state: GameState,
    game_started: bool,
}

impl GameRoom {
    fn new() -> Self {
        GameRoom {
            game_state: GameState {
                board: [[0; 8]; 8],
                current_turn: 1, // Começa com o jogador 1 (X)
            },
            game_started: false,
        }
    }

    fn update_game_state(&mut self, player_move: Move) {
        println!("Jogada recebida: ({}, {})", player_move.row, player_move.col);

        if self.game_state.board[player_move.row][player_move.col] == 0 {
            self.game_state.board[player_move.row][player_move.col] = self.game_state.current_turn;
            self.game_state.current_turn *= -1; // Troca de turno
            println!("Tabuleiro atualizado: {:?}", self.game_state.board);
        } else {
            println!("Jogada inválida! A posição já está ocupada.");
        }
    }

    fn start_game(&mut self) {
        if !self.game_started {
            self.game_started = true;
            println!("O jogo começou!");
        }
    }

    fn get_game_state(&self) -> String {
        let mut board_str = String::new();
        for row in &self.game_state.board {
            for &cell in row {
                board_str.push_str(&format!("{} ", if cell == 1 { "X" } else if cell == -1 { "O" } else { "." }));
            }
            board_str.push_str("\n");
        }
        board_str.push_str(&format!("Vez do jogador: {}", if self.game_state.current_turn == 1 { "Jogador 1 (X)" } else { "Jogador 2 (O)" }));
        board_str
    }
}

async fn handle_client(stream: tokio::net::TcpStream, game_room: Arc<Mutex<GameRoom>>) {
    let (reader, mut writer) = io::split(stream);
    let mut reader = io::BufReader::new(reader);
    let mut buffer = String::new();

    // Enviar o estado inicial do jogo
    {
        let game_room_lock = game_room.lock().await;
        let initial_message = serde_json::to_string(&game_room_lock.game_state).unwrap();
        // Escreva para o stream após o lock ser liberado
        let _ = writer.write_all(initial_message.as_bytes()).await;
    }

    loop {
        // Exibir o estado atual do jogo no Telnet
        let game_state_str = {
            let game_room_lock = game_room.lock().await;
            game_room_lock.get_game_state()
        };
        // Escreva o estado do jogo para o stream
        let _ = writer.write_all(game_state_str.as_bytes()).await;

        // Esperar pela jogada do jogador
        buffer.clear();
        if let Err(_) = reader.read_line(&mut buffer).await {
            break;
        }

        let parts: Vec<&str> = buffer.trim().split_whitespace().collect();
        if parts.len() == 2 {
            if let (Ok(row), Ok(col)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
                let player_move = Move { row, col };
                let mut game_room_lock = game_room.lock().await;
                game_room_lock.update_game_state(player_move);
            }
        }

        // Verificar se o jogo já terminou ou se alguém venceu
        // (Isso pode ser expandido para verificar o vencedor, etc.)
    }
}

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Servidor iniciado na porta 8080");

    let game_room = Arc::new(Mutex::new(GameRoom::new()));  // Use Arc<Mutex<GameRoom>>

    while let Ok((stream, _)) = listener.accept().await {
        let game_room_clone = Arc::clone(&game_room);  // Clonamos o Arc, não movemos o valor

        tokio::spawn(async move {
            println!("Novo cliente conectado");

            let mut game_room_lock = game_room_clone.lock().await;
            game_room_lock.start_game();

            drop(game_room_lock); // Drop the lock before calling handle_client
            handle_client(stream, game_room_clone).await;
        });
    }
}
