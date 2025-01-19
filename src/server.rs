use tokio_tungstenite::{tungstenite::protocol::Message, accept_async};
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;

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
    players: Vec<mpsc::Sender<GameState>>,
    game_state: GameState,
    player_count: usize,
    game_started: bool,
}

impl GameRoom {
    fn new() -> Self {
        GameRoom {
            players: Vec::new(),
            game_state: GameState {
                board: [[0; 8]; 8],
                current_turn: 1,
            },
            player_count: 0, // Nenhum jogador inicialmente
            game_started: false,
        }
    }

    fn add_player(&mut self, player: mpsc::Sender<GameState>) {
        self.players.push(player);
        self.player_count += 1;
    }

    fn update_game_state(&mut self, player_move: Move) {
        // Verifica se a jogada é válida
        if self.game_state.board[player_move.row][player_move.col] == 0 {
            self.game_state.board[player_move.row][player_move.col] = self.game_state.current_turn;
            self.game_state.current_turn *= -1; // Troca de turno
        }
    }

    fn broadcast(&self) {
        for player in &self.players {
            let _ = player.send(self.game_state.clone());
        }
    }

    fn start_game(&mut self) {
        // Inicia o jogo quando ambos os jogadores estiverem conectados
        if self.player_count == 2 && !self.game_started {
            self.game_started = true;
            // Envia uma mensagem de "início de jogo" para ambos os jogadores
            let start_message = "O jogo começou!";
            for player in &self.players {
                let _ = player.send(self.game_state.clone());
            }
            println!("{}", start_message); // Imprimir no servidor que o jogo começou
        }
    }
}

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("Servidor iniciado na porta 8080");

    // Usar Arc<Mutex> para compartilhar o estado do jogo entre threads
    let game_room = Arc::new(Mutex::new(GameRoom::new()));

    while let Ok((stream, _)) = listener.accept().await {
        // Criar um canal para cada jogador
        let (tx, _rx) = mpsc::channel(1);

        // Clonar o Arc para passar para a thread
        let game_room_clone = game_room.clone();
        tokio::spawn(async move {
            let mut ws_stream = accept_async(stream)
                .await
                .expect("Erro ao aceitar a conexão");
            println!("Novo cliente conectado");

            // Enviar estado inicial do jogo
            let game_room = game_room_clone.lock().await;
            let initial_state = game_room.game_state.clone();
            let initial_message = serde_json::to_string(&initial_state).unwrap();
            let _ = ws_stream.send(Message::Text(initial_message)).await;

            // Esperar o segundo jogador se conectar e iniciar o jogo
            let mut game_room = game_room_clone.lock().await;
            game_room.start_game(); // Verifica se o segundo jogador está presente

            // Receber e processar mensagens dos jogadores
            while let Some(Ok(msg)) = ws_stream.next().await {
                if let Message::Text(msg_text) = msg {
                    let player_move: Move = serde_json::from_str(&msg_text).unwrap();

                    // Atualizar o estado do jogo com a jogada
                    let mut game_room = game_room_clone.lock().await;
                    game_room.update_game_state(player_move);
                    game_room.broadcast(); // Enviar a atualização para os outros jogadores
                }
            }
        });

        // Adiciona o jogador à sala de jogo
        let mut game_room = game_room.lock().await;
        game_room.add_player(tx);

        // Imprimir a quantidade de jogadores conectados
        println!("Clientes conectados: {}", game_room.player_count);

        // Caso já haja 2 jogadores, inicie o jogo
        game_room.start_game();
    }
}
