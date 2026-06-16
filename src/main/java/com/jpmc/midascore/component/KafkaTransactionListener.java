package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        // Extracting the data using getter methods
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        // Printing the actual data to the console
        System.out.println("Received Transaction:");
        System.out.println("Sender ID: " + senderId);
        System.out.println("Recipient ID: " + recipientId);
        System.out.println("Amount: " + amount);
        System.out.println("-------------------------");
    }
}
