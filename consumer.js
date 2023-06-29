import amqp from 'amqplib';

const rabbitSettings = {
  protocol: 'amqp',
  hostname: '3.215.52.131',
  port: 5672,
  username: 'David',
  password: 'cereza23',
};

async function consumeMessages() {
  try {
    const conn = await amqp.connect(rabbitSettings);
    console.log('Conexión exitosa');

    const channel = await conn.createChannel();
    console.log('Canal creado');

    const queue = 'Video';

    await channel.assertQueue(queue);
    console.log('Cola creada');

    channel.consume(queue, async (message) => {
      if (message !== null) {
        const messageContent = message.content.toString();
        console.log('Mensaje recibido:', messageContent);

        try {
          const apiUrl = 'http://localhost:4000/video';
          const options = {
            method: 'POST',
            body: JSON.stringify({ Title: messageContent }),
            headers: {
              'Content-Type': 'application/json'
            }
          };

          const response = await fetch(apiUrl, options);
          const data = await response.json();
          console.log('Respuesta de la API:', data);

          // Eliminar el mensaje de la cola una vez procesado
          channel.ack(message);
          console.log('Mensaje eliminado de la cola');
        } catch (error) {
          console.error('Error al hacer la solicitud a la API:', error);
          // Rechazar (rechazar) el mensaje sin volver a encolarlo
          channel.reject(message, false);
          console.log('Error al procesar el mensaje, se elimina de la cola');
        }
      }
    });

  } catch (error) {
    console.error('Error:', error);
  }
}

consumeMessages();