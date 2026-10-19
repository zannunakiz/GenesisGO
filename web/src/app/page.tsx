import styles from "./page.module.css";

export default function Home() {
  return (
    <main className={styles.page}>
      <section className={styles.content} aria-labelledby="page-title">
        <p className={styles.brand}>GenesisGo</p>
        <h1 id="page-title">Event reliability testing</h1>
        <p className={styles.description}>
          A local-first workspace for exploring reliability in event-driven
          systems.
        </p>
        <p className={styles.status} role="status">
          Scenario execution is not available yet.
        </p>
      </section>
    </main>
  );
}
