package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaTransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        // 1. Fetch users directly (The JPMC repository returns UserRecord or null)
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // 2. Validate that both users exist (neither is null)
        if (sender != null && recipient != null) {

            // 3. Validate that the sender has enough money and amount is positive
            if (sender.getBalance() >= transaction.getAmount() && transaction.getAmount() > 0) {

                // 4. Update balances
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                // 5. Save updated users back to the database
                userRepository.save(sender);
                userRepository.save(recipient);

                // 6. Create and save the permanent transaction record
                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
                transactionRecordRepository.save(record);

                // --- ADD THESE LINES TO FIND WALDORF'S BALANCE ---
                System.out.println("Transaction Saved!");
                System.out.println("Sender: " + sender.getName() + " | New Balance: " + sender.getBalance());
                System.out.println("Recipient: " + recipient.getName() + " | New Balance: " + recipient.getBalance());
                System.out.println("-------------------------------------------------");
                System.out.println("Valid Transaction Processed and Saved!");
            } else {
                System.out.println("Invalid Transaction: Insufficient funds or invalid amount.");
            }
        } else {
            System.out.println("Invalid Transaction: Sender or Recipient does not exist.");
        }
    }
}