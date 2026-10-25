'use client'

import { useEffect, useState } from 'react'
import 'swagger-ui-dist/swagger-ui.css'
import styles from '../page.module.css'

export default function ApiDocsPage() {
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let mounted = true
    import('swagger-ui-dist')
      .then(({ SwaggerUIBundle }) => {
        if (!mounted) return
        SwaggerUIBundle({
          url: '/openapi.yaml',
          dom_id: '#swagger-ui',
          deepLinking: true,
          displayRequestDuration: true,
          filter: true,
          persistAuthorization: false,
          tryItOutEnabled: true,
        })
      })
      .catch(() => {
        if (mounted) setError('The API reference failed to initialize.')
      })
    return () => { mounted = false }
  }, [])

  return (
    <main className={styles.apiDocsPage}>
      <header className={styles.apiDocsHeader}>
        <a href="/">GenesisGo</a>
        <h1>API reference</h1>
        <p>Interactive local API contract. Authorization is not saved.</p>
      </header>
      {error && <p role="alert">{error}</p>}
      <div id="swagger-ui" />
    </main>
  )
}
