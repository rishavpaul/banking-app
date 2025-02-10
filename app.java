import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/* ────────────────────────────────────────────── *
 *                   BANK INTERFACE              *
 * ────────────────────────────────────────────── *
 * Defines the contract for our banking system.  *
 * Each Bank implementation provides its own      *
 * Transaction inner class with isolation control. *
 * Emojis & ASCII art added for fun! 🎉💰         *
 * ────────────────────────────────────────────── */
interface Bank {
  // Begin a new transaction.
  Transaction beginTransaction();
  // Create a new account with an initial balance.
  void createAccount(String accountId, double initialBalance);
  // For testing purposes: get the current account object.
  Account getAccount(String accountId);
  // Query accounts by minimum balance (for phantom-read demonstration).
  List<String> getAccountsByBalance(double minBalance);

  // Transaction interface – operations done inside a transaction.
  interface Transaction {
    void deposit(String accountId, double amount);
    void withdraw(String accountId, double amount);
    void transfer(String fromAccountId, String toAccountId, double amount);
    double getBalance(String accountId);
    List<String> getAccountsByBalance(double minBalance);
    void commit();
  }
}

/* ────────────────────────────────────────────── *
 *                    ACCOUNT CLASS              *
 * ────────────────────────────────────────────── *
 * Simple POJO for an account. Each account holds   *
 * a balance and a version (to help with MVCC).       *
 * ────────────────────────────────────────────── */
class Account {
  public String accountId;
  public double balance;
  public int version;

  public Account(String accountId, double balance) {
    this.accountId = accountId;
    this.balance = balance;
    this.version = 0;
  }
}

/* ────────────────────────────────────────────── *
 *              ABSTRACT BANK BASE               *
 * ────────────────────────────────────────────── *
 * Provides common functionality for all bank types *
 * (e.g. storing accounts in a thread-safe map).     *
 * ────────────────────────────────────────────── */
abstract class AbstractBank implements Bank {
  protected final ConcurrentHashMap<String, Account> accounts = new ConcurrentHashMap<>();

  @Override
  public void createAccount(String accountId, double initialBalance) {
    accounts.put(accountId, new Account(accountId, initialBalance));
  }

  @Override
  public Account getAccount(String accountId) {
    return accounts.get(accountId);
  }

  @Override
  public List<String> getAccountsByBalance(double minBalance) {
    List<String> result = new ArrayList<>();
    for (Account acc : accounts.values()) {
      if (acc.balance >= minBalance) {
        result.add(acc.accountId);
      }
    }
    return result;
  }
}

/* ────────────────────────────────────────────── *
 *             RANGE LOCK MANAGER                *
 * ────────────────────────────────────────────── *
 * A simple lock manager to simulate range locking. *
 * When a transaction executes a range query, it    *
 * acquires a lock on that query condition (e.g.,     *
 * "balance >= 800"). This prevents concurrent inserts *
 * from adding new rows that qualify for the range.   *
 * ────────────────────────────────────────────── */
class RangeLockManager {
  private final ConcurrentHashMap<String, ReentrantLock> rangeLocks = new ConcurrentHashMap<>();

  public void acquireRangeLock(String rangeKey) {
    rangeLocks.putIfAbsent(rangeKey, new ReentrantLock());
    ReentrantLock lock = rangeLocks.get(rangeKey);
    System.out.println("🔒 [RangeLockManager] Acquiring range lock for condition \"" + rangeKey + "\"");
    lock.lock();
    System.out.println("🔒 [RangeLockManager] Acquired range lock for condition \"" + rangeKey + "\"");
  }

  public void releaseRangeLock(String rangeKey) {
    ReentrantLock lock = rangeLocks.get(rangeKey);
    if (lock != null && lock.isHeldByCurrentThread()) {
      lock.unlock();
      System.out.println("🔓 [RangeLockManager] Released range lock for condition \"" + rangeKey + "\"");
    }
  }

  /**
   * For new account insertion, if there is any active range lock whose condition
   * the new account qualifies for, block until that lock is released.
   */
  public void waitForRangeLocksForInsert(double balance) {
    for (Map.Entry<String, ReentrantLock> entry : rangeLocks.entrySet()) {
      String key = entry.getKey();  // e.g., ">=800.0"
      if (key.startsWith(">=")) {
        double threshold = Double.parseDouble(key.substring(2));
        if (balance >= threshold) {
          System.out.println("⏳ [RangeLockManager] New account with balance " + balance +
            " qualifies for range lock \"" + key + "\". Waiting...");
          ReentrantLock lock = entry.getValue();
          // This call will block if the lock is held by a transaction.
          lock.lock();
          lock.unlock();
          System.out.println("✅ [RangeLockManager] New account proceeding after waiting on lock \"" + key + "\"");
        }
      }
    }
  }
}

/* ────────────────────────────────────────────── *
 *         1. READ UNCOMMITTED (RU) BANK         *
 * ────────────────────────────────────────────── *
 * In RU, every change is immediately applied –    *
 * there is no isolation. Dirty reads can occur!    *
 * (Not recommended for banks! ⚠️)                  *
 * ────────────────────────────────────────────── */
class BankReadUncommitted extends AbstractBank {

  @Override
  public Transaction beginTransaction() {
    return new TransactionRU();
  }

  class TransactionRU implements Bank.Transaction {
    // No local buffering; every change is applied to the global state.
    @Override
    public void deposit(String accountId, double amount) {
      Account acc = accounts.get(accountId);
      if (acc != null) {
        synchronized(acc) {
          System.out.println("RU: Depositing " + amount + " to " + accountId +
            " (before: " + acc.balance + ") 💸");
          acc.balance += amount;
          acc.version++;
          System.out.println("RU: New balance: " + acc.balance + " 🚀");
        }
      }
    }

    @Override
    public void withdraw(String accountId, double amount) {
      Account acc = accounts.get(accountId);
      if (acc != null) {
        synchronized(acc) {
          System.out.println("RU: Withdrawing " + amount + " from " + accountId +
            " (before: " + acc.balance + ") 💸");
          acc.balance -= amount;
          acc.version++;
          System.out.println("RU: New balance: " + acc.balance + " 🚀");
        }
      }
    }

    @Override
    public void transfer(String fromAccountId, String toAccountId, double amount) {
      // To avoid deadlocks in this simulation, lock in a consistent order.
      if (fromAccountId.compareTo(toAccountId) < 0) {
        withdraw(fromAccountId, amount);
        deposit(toAccountId, amount);
      } else {
        deposit(toAccountId, amount);
        withdraw(fromAccountId, amount);
      }
    }

    @Override
    public double getBalance(String accountId) {
      Account acc = accounts.get(accountId);
      if (acc != null) {
        synchronized(acc) {
          System.out.println("RU: Reading balance for " + accountId +
            ": " + acc.balance + " 👀");
          return acc.balance;
        }
      }
      return 0;
    }

    @Override
    public List<String> getAccountsByBalance(double minBalance) {
      List<String> result = new ArrayList<>();
      for (Account acc : accounts.values()) {
        synchronized(acc) {
          if (acc.balance >= minBalance) {
            result.add(acc.accountId);
          }
        }
      }
      System.out.println("RU: Accounts with balance >= " + minBalance +
        ": " + result + " 🔍");
      return result;
    }

    @Override
    public void commit() {
      // In RU, changes are immediately visible.
      System.out.println("RU: Commit called (no-op, changes are already visible) ✅");
    }
  }
}

/* ────────────────────────────────────────────── *
 *         2. READ COMMITTED (RC) BANK           *
 * ────────────────────────────────────────────── *
 * In RC, write operations are buffered until       *
 * commit. Reads always see only committed data, but  *
 * non-repeatable reads may occur.                  *
 * ────────────────────────────────────────────── */
class BankReadCommitted extends AbstractBank {

  @Override
  public Transaction beginTransaction() {
    return new TransactionRC();
  }

  class TransactionRC implements Bank.Transaction {
    // Local write buffer: accountId -> new balance.
    private final HashMap<String, Double> writeBuffer = new HashMap<>();

    @Override
    public void deposit(String accountId, double amount) {
      double current = getBalance(accountId);
      double newBalance = current + amount;
      writeBuffer.put(accountId, newBalance);
      System.out.println("RC: Buffered deposit " + amount + " to " + accountId +
        " => " + newBalance + " 💰");
    }

    @Override
    public void withdraw(String accountId, double amount) {
      double current = getBalance(accountId);
      double newBalance = current - amount;
      writeBuffer.put(accountId, newBalance);
      System.out.println("RC: Buffered withdrawal " + amount + " from " + accountId +
        " => " + newBalance + " 💸");
    }

    @Override
    public void transfer(String fromAccountId, String toAccountId, double amount) {
      withdraw(fromAccountId, amount);
      deposit(toAccountId, amount);
    }

    @Override
    public double getBalance(String accountId) {
      if (writeBuffer.containsKey(accountId)) {
        double bal = writeBuffer.get(accountId);
        System.out.println("RC: Reading buffered balance for " + accountId +
          ": " + bal + " 👀");
        return bal;
      } else {
        Account acc = accounts.get(accountId);
        if (acc != null) {
          synchronized(acc) {
            System.out.println("RC: Reading committed balance for " + accountId +
              ": " + acc.balance + " 👀");
            return acc.balance;
          }
        }
      }
      return 0;
    }

    @Override
    public List<String> getAccountsByBalance(double minBalance) {
      // For simplicity, read directly from the global committed state.
      List<String> result = new ArrayList<>();
      for (Account acc : accounts.values()) {
        synchronized(acc) {
          if (acc.balance >= minBalance) {
            result.add(acc.accountId);
          }
        }
      }
      System.out.println("RC: Query accounts with balance >= " + minBalance +
        ": " + result + " 🔍");
      return result;
    }

    @Override
    public void commit() {
      // Atomically apply the buffered changes.
      for (Map.Entry<String, Double> entry : writeBuffer.entrySet()) {
        Account acc = accounts.get(entry.getKey());
        if (acc != null) {
          synchronized(acc) {
            System.out.println("RC: Committing " + entry.getKey() +
              " new balance: " + entry.getValue() + " ✅");
            acc.balance = entry.getValue();
            acc.version++;
          }
        }
      }
      writeBuffer.clear();
      System.out.println("RC: Transaction committed. ✅");
    }
  }
}

/* ────────────────────────────────────────────── *
 *         3. REPEATABLE READ (RR – MVCC)          *
 * ────────────────────────────────────────────── *
 * Here each transaction takes a snapshot of the    *
 * global state at start. Reads return snapshot data  *
 * even if other transactions commit concurrently.    *
 * Conflicts (based on version numbers) are checked   *
 * at commit time. This prevents non-repeatable reads,  *
 * though phantom anomalies may still occur in range    *
 * queries if the query scans the live data.          *
 * ────────────────────────────────────────────── */
class BankRepeatableRead extends AbstractBank {

  @Override
  public Transaction beginTransaction() {
    return new TransactionRR();
  }

  // Simple snapshot representation.
  class AccountSnapshot {
    double balance;
    int version;

    AccountSnapshot(double balance, int version) {
      this.balance = balance;
      this.version = version;
    }
  }

  class TransactionRR implements Bank.Transaction {
    // Snapshot of accounts at transaction start.
    private final HashMap<String, AccountSnapshot> snapshot = new HashMap<>();
    // Local write buffer.
    private final HashMap<String, Double> writeBuffer = new HashMap<>();

    public TransactionRR() {
      // Take a snapshot of every account.
      for (Map.Entry<String, Account> entry : accounts.entrySet()) {
        Account acc = entry.getValue();
        synchronized(acc) {
          snapshot.put(entry.getKey(), new AccountSnapshot(acc.balance, acc.version));
        }
      }
      System.out.println("RR: Snapshot taken: " + snapshotInfo() + " 📸");
    }

    private String snapshotInfo() {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, AccountSnapshot> e : snapshot.entrySet()) {
        sb.append(e.getKey()).append("->(")
          .append(e.getValue().balance).append(", v")
          .append(e.getValue().version).append(") ");
      }
      return sb.toString();
    }

    @Override
    public void deposit(String accountId, double amount) {
      double current = getBalance(accountId);
      double newBalance = current + amount;
      writeBuffer.put(accountId, newBalance);
      System.out.println("RR: Buffered deposit " + amount + " to " + accountId +
        " => " + newBalance + " 💰");
    }

    @Override
    public void withdraw(String accountId, double amount) {
      double current = getBalance(accountId);
      double newBalance = current - amount;
      writeBuffer.put(accountId, newBalance);
      System.out.println("RR: Buffered withdrawal " + amount + " from " + accountId +
        " => " + newBalance + " 💸");
    }

    @Override
    public void transfer(String fromAccountId, String toAccountId, double amount) {
      withdraw(fromAccountId, amount);
      deposit(toAccountId, amount);
    }

    @Override
    public double getBalance(String accountId) {
      if (writeBuffer.containsKey(accountId)) {
        double bal = writeBuffer.get(accountId);
        System.out.println("RR: Reading buffered balance for " + accountId +
          ": " + bal + " 👀");
        return bal;
      } else if (snapshot.containsKey(accountId)) {
        double bal = snapshot.get(accountId).balance;
        System.out.println("RR: Reading snapshot balance for " + accountId +
          ": " + bal + " 👀");
        return bal;
      } else {
        System.out.println("RR: Account " + accountId + " not found in snapshot. 👻");
        return 0;
      }
    }

    @Override
    public List<String> getAccountsByBalance(double minBalance) {
      // Use the snapshot for a consistent query.
      List<String> result = new ArrayList<>();
      for (Map.Entry<String, AccountSnapshot> entry : snapshot.entrySet()) {
        double bal = entry.getValue().balance;
        // If there is a local update, use that.
        if (writeBuffer.containsKey(entry.getKey())) {
          bal = writeBuffer.get(entry.getKey());
        }
        if (bal >= minBalance) {
          result.add(entry.getKey());
        }
      }
      System.out.println("RR: Query snapshot accounts with balance >= " + minBalance +
        ": " + result + " 🔍");
      return result;
    }

    /**
     * This method bypasses the snapshot and directly scans the live global state.
     * It is used to simulate a phantom read scenario for range queries.
     */
    public List<String> getAccountsByBalancePhantom(double minBalance) {
      List<String> result = new ArrayList<>();
      for (Map.Entry<String, Account> entry : accounts.entrySet()) {
        Account acc = entry.getValue();
        synchronized(acc) {
          // If a local update exists, use it.
          double bal = writeBuffer.containsKey(entry.getKey())
            ? writeBuffer.get(entry.getKey())
            : acc.balance;
          if (bal >= minBalance) {
            result.add(entry.getKey());
          }
        }
      }
      System.out.println("RR (Phantom Query): Accounts with balance >= " + minBalance +
        ": " + result + " 🔍");
      return result;
    }

    @Override
    public void commit() {
      // Check for conflicts: if an account's version has changed since the snapshot.
      for (String accountId : writeBuffer.keySet()) {
        Account acc = accounts.get(accountId);
        if (acc != null) {
          synchronized(acc) {
            AccountSnapshot snap = snapshot.get(accountId);
            if (snap != null && acc.version != snap.version) {
              System.out.println("RR: Conflict detected on account " + accountId +
                "! (Snapshot v" + snap.version + ", current v" +
                acc.version + ") 🚨");
              System.out.println("RR: Transaction aborted due to conflict. ❌");
              return; // In real systems, we would abort or retry.
            }
          }
        }
      }
      // No conflicts? Apply the changes.
      for (Map.Entry<String, Double> entry : writeBuffer.entrySet()) {
        Account acc = accounts.get(entry.getKey());
        if (acc != null) {
          synchronized(acc) {
            System.out.println("RR: Committing " + entry.getKey() +
              " new balance: " + entry.getValue() + " ✅");
            acc.balance = entry.getValue();
            acc.version++;
          }
        }
      }
      System.out.println("RR: Transaction committed successfully. ✅");
    }
  }
}

/* ────────────────────────────────────────────── *
 *         4. SERIALIZABLE (SER) BANK            *
 * ────────────────────────────────────────────── *
 * This implementation uses strict two-phase locking. *
 * Locks are acquired on first access and held until  *
 * commit. No anomalies occur, but concurrency is     *
 * reduced. (This is why banks prefer serializable.)   *
 * ────────────────────────────────────────────── */
class BankSerializable extends AbstractBank {
  // Global lock map for each account.
  private final ConcurrentHashMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();

  public BankSerializable() {
    // Locks are created on demand.
  }

  private ReentrantLock getLock(String accountId) {
    lockMap.putIfAbsent(accountId, new ReentrantLock());
    return lockMap.get(accountId);
  }

  @Override
  public Transaction beginTransaction() {
    return new TransactionS();
  }

  class TransactionS implements Bank.Transaction {
    // Keep track of acquired locks.
    private final Set<String> acquiredLocks = new HashSet<>();

    private void acquireLock(String accountId) {
      if (!acquiredLocks.contains(accountId)) {
        ReentrantLock lock = getLock(accountId);
        lock.lock();
        acquiredLocks.add(accountId);
        System.out.println("SER: Acquired lock on " + accountId + " 🔒");
      }
    }

    @Override
    public void deposit(String accountId, double amount) {
      acquireLock(accountId);
      Account acc = accounts.get(accountId);
      if (acc != null) {
        System.out.println("SER: Depositing " + amount + " to " + accountId +
          " (before: " + acc.balance + ") 💰");
        acc.balance += amount;
        acc.version++;
        System.out.println("SER: New balance: " + acc.balance + " 🚀");
      }
    }

    @Override
    public void withdraw(String accountId, double amount) {
      acquireLock(accountId);
      Account acc = accounts.get(accountId);
      if (acc != null) {
        System.out.println("SER: Withdrawing " + amount + " from " + accountId +
          " (before: " + acc.balance + ") 💸");
        acc.balance -= amount;
        acc.version++;
        System.out.println("SER: New balance: " + acc.balance + " 🚀");
      }
    }

    @Override
    public void transfer(String fromAccountId, String toAccountId, double amount) {
      // Acquire locks in a consistent order.
      if (fromAccountId.compareTo(toAccountId) < 0) {
        acquireLock(fromAccountId);
        acquireLock(toAccountId);
      } else {
        acquireLock(toAccountId);
        acquireLock(fromAccountId);
      }
      withdraw(fromAccountId, amount);
      deposit(toAccountId, amount);
    }

    @Override
    public double getBalance(String accountId) {
      acquireLock(accountId);
      Account acc = accounts.get(accountId);
      if (acc != null) {
        System.out.println("SER: Reading balance for " + accountId +
          ": " + acc.balance + " 👀");
        return acc.balance;
      }
      return 0;
    }

    @Override
    public List<String> getAccountsByBalance(double minBalance) {
      // For a consistent view, acquire locks on all accounts.
      for (String accountId : accounts.keySet()) {
        acquireLock(accountId);
      }
      List<String> result = new ArrayList<>();
      for (Account acc : accounts.values()) {
        if (acc.balance >= minBalance) {
          result.add(acc.accountId);
        }
      }
      System.out.println("SER: Query accounts with balance >= " + minBalance +
        ": " + result + " 🔍");
      return result;
    }

    @Override
    public void commit() {
      // Changes have been applied immediately. Now release all locks.
      for (String accountId : acquiredLocks) {
        ReentrantLock lock = getLock(accountId);
        lock.unlock();
        System.out.println("SER: Released lock on " + accountId + " 🔓");
      }
      acquiredLocks.clear();
      System.out.println("SER: Transaction committed. ✅");
    }
  }
}

/* ──────────────────────────────────────────────── *
 *   5. MVCC WITH RANGE LOCKS (SERIALIZABLE) BANK   *
 * ──────────────────────────────────────────────── *
 * This implementation uses MVCC with a snapshot and  *
 * additionally acquires range locks on queries to   *
 * prevent phantom reads. New inserts that would add   *
 * phantom rows are blocked until the transaction     *
 * completes. This is why banks prefer Serializable      *
 * isolation. 🚀                                     *
 * ────────────────────────────────────────────── */
class BankMVCCSerializable extends AbstractBank {
  private final RangeLockManager rangeLockManager = new RangeLockManager();

  // Override createAccount to wait for any conflicting range locks.
  @Override
  public void createAccount(String accountId, double initialBalance) {
    // Before creating an account, wait if any active range locks apply.
    rangeLockManager.waitForRangeLocksForInsert(initialBalance);
    super.createAccount(accountId, initialBalance);
    System.out.println("MVCC-SER: Created account " + accountId + " with balance " + initialBalance + " ✅");
  }

  @Override
  public Transaction beginTransaction() {
    return new TransactionMVCCSerializable();
  }

  // A simple snapshot representation for MVCC.
  class AccountSnapshot {
    double balance;
    int version;

    AccountSnapshot(double balance, int version) {
      this.balance = balance;
      this.version = version;
    }
  }

  class TransactionMVCCSerializable implements Bank.Transaction {
    // Take a snapshot of accounts at the transaction’s start.
    private final HashMap<String, AccountSnapshot> snapshot = new HashMap<>();
    // Local write buffer for uncommitted changes.
    private final HashMap<String, Double> writeBuffer = new HashMap<>();
    // Keep track of acquired range locks (by their condition key).
    private final Set<String> acquiredRangeLocks = new HashSet<>();

    public TransactionMVCCSerializable() {
      for (Map.Entry<String, Account> entry : accounts.entrySet()) {
        Account acc = entry.getValue();
        synchronized(acc) {
          snapshot.put(entry.getKey(), new AccountSnapshot(acc.balance, acc.version));
        }
      }
      System.out.println("MVCC-SER: Snapshot taken: " + snapshotInfo() + " 📸");
    }

    private String snapshotInfo() {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, AccountSnapshot> e : snapshot.entrySet()) {
        sb.append(e.getKey()).append("->(")
          .append(e.getValue().balance).append(", v")
          .append(e.getValue().version).append(") ");
      }
      return sb.toString();
    }

    @Override
    public void deposit(String accountId, double amount) {
      double current = getBalance(accountId);
      double newBalance = current + amount;
      writeBuffer.put(accountId, newBalance);
      System.out.println("MVCC-SER: Buffered deposit " + amount + " to " + accountId +
        " => " + newBalance + " 💰");
    }

    @Override
    public void withdraw(String accountId, double amount) {
      double current = getBalance(accountId);
      double newBalance = current - amount;
      writeBuffer.put(accountId, newBalance);
      System.out.println("MVCC-SER: Buffered withdrawal " + amount + " from " + accountId +
        " => " + newBalance + " 💸");
    }

    @Override
    public void transfer(String fromAccountId, String toAccountId, double amount) {
      withdraw(fromAccountId, amount);
      deposit(toAccountId, amount);
    }

    @Override
    public double getBalance(String accountId) {
      if (writeBuffer.containsKey(accountId)) {
        double bal = writeBuffer.get(accountId);
        System.out.println("MVCC-SER: Reading buffered balance for " + accountId +
          ": " + bal + " 👀");
        return bal;
      } else if (snapshot.containsKey(accountId)) {
        double bal = snapshot.get(accountId).balance;
        System.out.println("MVCC-SER: Reading snapshot balance for " + accountId +
          ": " + bal + " 👀");
        return bal;
      } else {
        System.out.println("MVCC-SER: Account " + accountId + " not found in snapshot. 👻");
        return 0;
      }
    }

    @Override
    public List<String> getAccountsByBalance(double minBalance) {
      // Acquire a range lock for this query to prevent phantom inserts.
      String rangeKey = ">=" + minBalance;
      if (!acquiredRangeLocks.contains(rangeKey)) {
        rangeLockManager.acquireRangeLock(rangeKey);
        acquiredRangeLocks.add(rangeKey);
      }
      // Use the snapshot for a consistent query.
      List<String> result = new ArrayList<>();
      for (Map.Entry<String, AccountSnapshot> entry : snapshot.entrySet()) {
        double bal = entry.getValue().balance;
        // If a local update exists, use it.
        if (writeBuffer.containsKey(entry.getKey())) {
          bal = writeBuffer.get(entry.getKey());
        }
        if (bal >= minBalance) {
          result.add(entry.getKey());
        }
      }
      System.out.println("MVCC-SER: Range query (balance >= " + minBalance +
        ") result: " + result + " 🔍");
      return result;
    }

    @Override
    public void commit() {
      // Conflict detection: ensure that no account's version has changed.
      for (String accountId : writeBuffer.keySet()) {
        Account acc = accounts.get(accountId);
        if (acc != null) {
          synchronized(acc) {
            AccountSnapshot snap = snapshot.get(accountId);
            if (snap != null && acc.version != snap.version) {
              System.out.println("MVCC-SER: Conflict detected on account " + accountId +
                "! (Snapshot v" + snap.version + ", current v" +
                acc.version + ") 🚨");
              System.out.println("MVCC-SER: Transaction aborted due to conflict. ❌");
              return; // In a real system, we would abort or retry.
            }
          }
        }
      }
      // Apply the buffered changes.
      for (Map.Entry<String, Double> entry : writeBuffer.entrySet()) {
        Account acc = accounts.get(entry.getKey());
        if (acc != null) {
          synchronized(acc) {
            System.out.println("MVCC-SER: Committing " + entry.getKey() +
              " new balance: " + entry.getValue() + " ✅");
            acc.balance = entry.getValue();
            acc.version++;
          }
        }
      }
      // Release all acquired range locks.
      for (String rangeKey : acquiredRangeLocks) {
        rangeLockManager.releaseRangeLock(rangeKey);
      }
      acquiredRangeLocks.clear();
      System.out.println("MVCC-SER: Transaction committed successfully. ✅");
    }
  }
}
/* ────────────────────────────────────────────── *
 *                   MAIN APP                    *
 * ────────────────────────────────────────────── *
 * This class runs detailed test cases to demonstrate *
 * the downsides of each isolation level. Notice how   *
 * banks prefer the SERIALIZABLE (SER) isolation level   *
 * because it avoids dirty reads, non-repeatable reads,   *
 * and phantom anomalies. Enjoy the ASCII art and emojis! 😎 *
 * ────────────────────────────────────────────── */
class BankingApp {
  public static void main(String[] args) {
    System.out.println("┌──────────────────────────────────────────────┐");
    System.out.println("│   Welcome to the Java Banking App Demo! 💰   │");
    System.out.println("└──────────────────────────────────────────────┘");

    // Test for Read Uncommitted (dirty reads)
    testReadUncommitted();

    // Test for Read Committed (non-repeatable reads)
    testReadCommitted();

    // Test for Repeatable Read (MVCC) including a phantom read scenario
    testRepeatableRead();

    // Test for Serializable (full isolation)
    testSerializable();

    // Test for MVCC with Range Locks (Serializable)
    testMVCCSerializable();
  }

  // ──────────── TEST: READ UNCOMMITTED ─────────────
  private static void testReadUncommitted() {
    System.out.println("\n========== Testing Read Uncommitted ==========");
    final BankReadUncommitted bank = new BankReadUncommitted();
    bank.createAccount("A", 1000.0);

    // Transaction T1: Withdraw from account A but delay commit to simulate an "in-progress" update.
    Thread t1 = new Thread(() -> {
      Bank.Transaction tx = bank.beginTransaction();
      tx.withdraw("A", 100.0);
      try {
        System.out.println("T1: Processing... (simulating delay) ⏳");
        Thread.sleep(2000); // Simulate long processing (dirty update still visible)
      } catch (InterruptedException e) {}
      tx.commit();
      System.out.println("T1: Committed. ✅");
    });

    // Transaction T2: Read the balance while T1 is still “in-flight.”
    Thread t2 = new Thread(() -> {
      try {
        Thread.sleep(500); // Ensure T1 has already done the withdrawal
      } catch (InterruptedException e) {}
      Bank.Transaction tx = bank.beginTransaction();
      double balance = tx.getBalance("A");
      System.out.println("T2: Read balance: " + balance +
        " (reflects dirty update in RU) 🚨");
      tx.commit();
    });

    t1.start();
    t2.start();
    try {
      t1.join();
      t2.join();
    } catch (InterruptedException e) {}
  }

  // ──────────── TEST: READ COMMITTED ─────────────
  private static void testReadCommitted() {
    System.out.println("\n========== Testing Read Committed ==========");
    final BankReadCommitted bank = new BankReadCommitted();
    bank.createAccount("B", 1000.0);

    // Transaction T1: Reads the balance twice with a delay.
    Thread t1 = new Thread(() -> {
      Bank.Transaction tx = bank.beginTransaction();
      double firstRead = tx.getBalance("B");
      System.out.println("T1: First read balance: " + firstRead + " 👀");
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {}
      double secondRead = tx.getBalance("B");
      System.out.println("T1: Second read balance: " + secondRead +
        " (non-repeatable read in RC) 🚨");
      tx.commit();
    });

    // Transaction T2: Deposits money in between T1's reads.
    Thread t2 = new Thread(() -> {
      try {
        Thread.sleep(700); // Ensure T1's first read occurs first.
      } catch (InterruptedException e) {}
      Bank.Transaction tx = bank.beginTransaction();
      tx.deposit("B", 200.0);
      tx.commit();
      System.out.println("T2: Deposited 200 and committed. ✅");
    });

    t1.start();
    t2.start();
    try {
      t1.join();
      t2.join();
    } catch (InterruptedException e) {}
  }

  // ──────────── TEST: REPEATABLE READ (MVCC) & PHANTOM READ SCENARIO ─────────────
  private static void testRepeatableRead() {
    System.out.println("\n========== Testing Repeatable Read (MVCC) ==========");
    final BankRepeatableRead bank = new BankRepeatableRead();
    // Create initial accounts.
    bank.createAccount("C", 1000.0);
    bank.createAccount("D", 700.0);  // Does not qualify for phantom query if threshold is 800.

    // Part 1: Repeatable read for a single account.
    Thread t1 = new Thread(() -> {
      Bank.Transaction tx = bank.beginTransaction();
      double firstRead = tx.getBalance("C");
      System.out.println("T1: First read balance for account C: " + firstRead + " 👀");
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {}
      double secondRead = tx.getBalance("C");
      System.out.println("T1: Second read balance for account C: " + secondRead +
        " (remains the same in RR) ✅");
      tx.commit();
    });

    // Part 2: Phantom read scenario using range query.
    Thread t2 = new Thread(() -> {
      // Cast the transaction to TransactionRR to access the phantom query method.
      Bank.Transaction tx = bank.beginTransaction();
      BankRepeatableRead.TransactionRR txRR = (BankRepeatableRead.TransactionRR) tx;
      // First range query using the phantom method (scans live data).
      List<String> initialQuery = txRR.getAccountsByBalancePhantom(800);
      System.out.println("T2: Initial phantom range query result: " + initialQuery + " 👀");
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {}
      // Second range query – if a new qualifying account is added, it will appear now.
      List<String> secondQuery = txRR.getAccountsByBalancePhantom(800);
      System.out.println("T2: Second phantom range query result: " + secondQuery +
        " (phantom read anomaly if different) 🚨");
      tx.commit();
    });

    // Part 3: In between T2's two queries, a new account is created that qualifies for the range.
    Thread t3 = new Thread(() -> {
      try {
        Thread.sleep(700);  // Ensure T2's first query happens.
      } catch (InterruptedException e) {}
      bank.createAccount("E", 900.0);
      System.out.println("T3: Created account E with balance 900.0 ✅");
    });

    t1.start();
    t2.start();
    t3.start();

    try {
      t1.join();
      t2.join();
      t3.join();
    } catch (InterruptedException e) {}
  }

  // ──────────── TEST: SERIALIZABLE ─────────────
  private static void testSerializable() {
    System.out.println("\n========== Testing Serializable ==========");
    final BankSerializable bank = new BankSerializable();
    bank.createAccount("D", 1000.0);
    bank.createAccount("E", 500.0);

    // Transaction T1: Transfers 200 from account D to account E.
    Thread t1 = new Thread(() -> {
      Bank.Transaction tx = bank.beginTransaction();
      tx.transfer("D", "E", 200.0);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      tx.commit();
      System.out.println("T1: Transferred 200 from D to E and committed. ✅");
    });

    // Transaction T2: Reads the balances concurrently.
    Thread t2 = new Thread(() -> {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {}
      Bank.Transaction tx = bank.beginTransaction();
      double balanceD = tx.getBalance("D");
      double balanceE = tx.getBalance("E");
      System.out.println("T2: Read balances - D: " + balanceD +
        ", E: " + balanceE + " (SER guarantees consistency) 🔍");
      tx.commit();
    });

    t1.start();
    t2.start();
    try {
      t1.join();
      t2.join();
    } catch (InterruptedException e) {}
  }

  private static void testMVCCSerializable() {
    System.out.println("\n========== Testing MVCC with Range Locks (Serializable) ==========");
    final BankMVCCSerializable bank = new BankMVCCSerializable();

    // Create initial accounts.
    bank.createAccount("C", 1000.0);
    bank.createAccount("D", 700.0);  // Does not qualify for a query with threshold 800.

    // Part 1: A transaction reading account C (for completeness).
    Thread t1 = new Thread(() -> {
      Bank.Transaction tx = bank.beginTransaction();
      double firstRead = tx.getBalance("C");
      System.out.println("T1: First read balance for account C: " + firstRead + " 👀");
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {}
      double secondRead = tx.getBalance("C");
      System.out.println("T1: Second read balance for account C: " + secondRead +
        " (remains consistent in MVCC-SER) ✅");
      tx.commit();
    });

    // Part 2: Transaction T2 performs a range query for accounts with balance >= 800.
    // With range locking, this query will block phantom inserts.
    Thread t2 = new Thread(() -> {
      Bank.Transaction tx = bank.beginTransaction();
      // T2 acquires a range lock for "balance >= 800" when executing the query.
      List<String> initialQuery = tx.getAccountsByBalance(800);
      System.out.println("T2: Initial range query result: " + initialQuery + " 👀");
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {}
      // T2 executes the same range query a second time.
      List<String> secondQuery = tx.getAccountsByBalance(800);
      System.out.println("T2: Second range query result: " + secondQuery +
        " (should be identical, no phantoms!) 🎉");
      tx.commit();
    });

    // Part 3: Meanwhile, Transaction T3 (or a thread) tries to create a new account that qualifies.
    // Because T2 holds a range lock for ">=800", T3’s creation will block until T2 commits.
    Thread t3 = new Thread(() -> {
      try {
        Thread.sleep(700);  // Ensure T2’s query happens first.
      } catch (InterruptedException e) {}
      bank.createAccount("E", 900.0);
      System.out.println("T3: Created account E with balance 900.0 ✅");
    });

    t1.start();
    t2.start();
    t3.start();

    try {
      t1.join();
      t2.join();
      t3.join();
    } catch (InterruptedException e) {}
  }
}
