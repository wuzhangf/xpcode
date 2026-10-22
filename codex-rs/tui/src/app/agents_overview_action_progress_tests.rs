//! Progress rendering and input isolation while lifecycle requests are pending.

use super::*;
use crate::app::agents_overview::tests::overview_thread;
use crate::chatwidget::tests::helpers::normalize_agent_center_snapshot;
use codex_app_server_protocol::ThreadStatus;
use futures::stream;
use pretty_assertions::assert_eq;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use std::collections::VecDeque;
use std::task::Poll;
use std::time::Duration;

#[tokio::test]
async fn modal_consumes_input_and_finishes_after_input_closes() -> color_eyre::Result<()> {
    let mut tui = crate::tui::test_support::make_test_tui()?;
    tui.pause_events();
    let (finished_input, wait_for_input) = tokio::sync::oneshot::channel();
    let mut finished_input = Some(finished_input);
    let mut events = VecDeque::from([
        tui::TuiEvent::Key(crossterm::event::KeyCode::Esc.into()),
        tui::TuiEvent::Paste("ignore this input".into()),
        tui::TuiEvent::Draw,
        tui::TuiEvent::Resize(ratatui::layout::Size::new(80, 24)),
        tui::TuiEvent::FocusGained,
    ]);
    let events = stream::poll_fn(move |_| {
        if let Some(event) = events.pop_front() {
            Poll::Ready(Some(event))
        } else {
            if let Some(finished_input) = finished_input.take() {
                let _ = finished_input.send(());
            }
            Poll::Ready(None)
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_lifecycle_modal(
            &mut tui,
            AgentsOverviewAction::Archive,
            &Paragraph::new("Agent command center"),
            async {
                wait_for_input.await.expect("all modal input was consumed");
                tokio::task::yield_now().await;
                "operation finished"
            },
            events,
        ),
    )
    .await??;
    assert_eq!(result, "operation finished");
    Ok(())
}

#[tokio::test]
async fn lifecycle_progress_wraps_at_narrow_widths() {
    let (app, _rx, _op_rx) = crate::app::tests::make_test_app_with_channels().await;
    let threads = ["First task", "Selected task", "Last task"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            overview_thread(
                ThreadId::from_u128(index as u128 + 1),
                /*parent_thread_id*/ None,
                name,
                ThreadStatus::Idle,
            )
        })
        .collect();
    let backdrop = app.agents_overview_view(threads, Some(ThreadId::from_u128(/*value*/ 2)));
    for (action, name) in [
        (AgentsOverviewAction::Archive, "archive_progress"),
        (AgentsOverviewAction::Delete, "delete_progress"),
    ] {
        let progress = LifecycleProgress(action);
        let progress = DialogOverlay {
            backdrop: RenderableItem::Borrowed(&backdrop),
            dialog: CenteredView(&progress),
        };
        let mut terminal = Terminal::new(TestBackend::new(36, 24)).unwrap();
        terminal
            .draw(|frame| progress.render(frame.area(), frame.buffer_mut()))
            .unwrap();
        insta::assert_snapshot!(
            name,
            normalize_agent_center_snapshot(terminal.backend().to_string())
        );
    }
}
